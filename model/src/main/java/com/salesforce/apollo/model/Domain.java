/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Migration;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachments;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.delphinius.ShardedOracle;
import com.salesforce.apollo.model.stereotomy.ShardedKERL;
import com.salesforce.apollo.state.Mutator;
import com.salesforce.apollo.state.SqlStateMachine;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.event.protobuf.InteractionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.services.proto.ProtoKERLAdapter;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.JDBCType;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.model.schema.tables.Member.MEMBER;
import static com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
import static java.nio.file.Path.of;

/**
 * An abstract sharded domain, top level, or sub domain. A domain minimally
 * consists of a managed KERL, ReBAC Oracle and the defined membership
 *
 * @author hal.hildebrand
 */
abstract public class Domain {

    private static final Logger log = LoggerFactory.getLogger(Domain.class);
    protected final CHOAM choam;
    protected final KERL commonKERL;
    protected final ControlledIdentifierMember member;
    protected final Mutator mutator;
    protected final Oracle oracle;
    protected final Parameters params;
    protected final SqlStateMachine sqlStateMachine;
    protected final Connection stateConnection;
    protected final Executor executor;
    public Domain(ControlledIdentifierMember member, Parameters.Builder params, String dbURL, Path checkpointBaseDir,
                  RuntimeParameters.Builder runtime, TransactionConfiguration txnConfig, Executor executor) {
        var paramsClone = params.clone();
        var runtimeClone = runtime.clone();
        this.member = member;
        this.executor = executor;
        var dir = checkpointBaseDir.toFile();
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IllegalArgumentException("Cannot create checkpoint base directory: " + checkpointBaseDir);
            }
        }
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Must be a directory: " + checkpointBaseDir);
        }
        var checkpointDir = new File(dir, qb64(member.getIdentifier().getDigest()));
        sqlStateMachine = new SqlStateMachine(dbURL, new Properties(), checkpointDir);

        paramsClone.getProducer().ethereal().setSigner(member);
        this.params = paramsClone.build(runtimeClone.setCheckpointer(sqlStateMachine.getCheckpointer())
                .setProcessor(sqlStateMachine.getExecutor())
                .setMember(member)
                .setRestorer(sqlStateMachine.getBootstrapper())
                .setKerl(() -> kerl())
                .setGenesisData(members -> genesisOf(members))
                .build());
        choam = new CHOAM(this.params);
        mutator = sqlStateMachine.getMutator(choam.getSession());
        stateConnection = sqlStateMachine.newConnection();
        this.oracle = new ShardedOracle(stateConnection, mutator, txnConfig.scheduler(), params.getSubmitTimeout(),
                txnConfig.executor());
        this.commonKERL = new ShardedKERL(stateConnection, mutator, txnConfig.scheduler(), params.getSubmitTimeout(),
                params.getDigestAlgorithm(), txnConfig.executor());
        log.info("Domain: {} member: {} db URL: {} checkpoint base dir: {}", this.params.context().getId(),
                member.getId(), dbURL, checkpointBaseDir);
    }

    public static void addMembers(Connection connection, List<byte[]> members, String state) {
        var context = DSL.using(connection, SQLDialect.H2);
        for (var m : members) {
            var id = context.insertInto(IDENTIFIER, IDENTIFIER.PREFIX)
                    .values(m)
                    .onDuplicateKeyIgnore()
                    .returning(IDENTIFIER.ID)
                    .fetchOne();
            if (id != null) {
                context.insertInto(MEMBER).set(MEMBER.IDENTIFIER, id.value1()).onConflictDoNothing().execute();
            }
        }
    }

    public static Txn boostrapMigration() {
        Map<Path, URL> resources = new HashMap<>();
        resources.put(of("/initialize.xml"), res("/initialize.xml"));
        resources.put(of("/stereotomy/initialize.xml"), res("/stereotomy/initialize.xml"));
        resources.put(of("/stereotomy/stereotomy.xml"), res("/stereotomy/stereotomy.xml"));
        resources.put(of("/stereotomy/uni-kerl.xml"), res("/stereotomy/uni-kerl.xml"));
        resources.put(of("/delphinius/initialize.xml"), res("/delphinius/initialize.xml"));
        resources.put(of("/delphinius/delphinius.xml"), res("/delphinius/delphinius.xml"));
        resources.put(of("/delphinius/delphinius-functions.xml"), res("/delphinius/delphinius-functions.xml"));
        resources.put(of("/model/model.xml"), res("/model/model.xml"));

        return Txn.newBuilder()
                .setMigration(Migration.newBuilder()
                        .setUpdate(Mutator.changeLog(resources, "/initialize.xml"))
                        .build())
                .build();
    }

    public static boolean isMember(DSLContext context, SelfAddressingIdentifier id) {
        final var idTable = com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
        return context.fetchExists(context.select(MEMBER.IDENTIFIER)
                .from(MEMBER)
                .join(idTable)
                .on(idTable.ID.eq(MEMBER.IDENTIFIER))
                .and(idTable.PREFIX.eq(id.getDigest().getBytes())));
    }

    public static Path tempDirOf(ControlledIdentifier<SelfAddressingIdentifier> id) {
        Path dir;
        try {
            dir = Files.createTempDirectory(qb64(id.getDigest()));
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create temporary directory", e);
        }
        dir.toFile().deleteOnExit();
        return dir;
    }

    private static URL res(String resource) {
        return Domain.class.getResource(resource);
    }

    public boolean activate(Member m) {
        if (!active()) {
            return params.runtime()
                    .foundation()
                    .getFoundation()
                    .getMembershipList()
                    .stream()
                    .map(d -> Digest.from(d))
                    .anyMatch(d -> m.getId().equals(d));
        }
        final var context = DSL.using(stateConnection, SQLDialect.H2);
        final var activeMember = isMember(context, new SelfAddressingIdentifier(m.getId()));

        return activeMember;
    }

    public boolean active() {
        return choam.active();
    }

    public Context<Member> getContext() {
        return choam.context();
    }

    /**
     * @return the RBAC Oracle
     */
    public Oracle getDelphi() {
        return oracle;
    }

    /**
     * @return the Identifier of the receiver
     */
    public Identifier getIdentifier() {
        return member.getIdentifier().getIdentifier();
    }

    /**
     * @return the adapter that provides raw Protobuf access to the underlying KERI
     * resolution
     */
    public ProtoKERLAdapter getKERLService() {
        return new ProtoKERLAdapter(commonKERL);
    }

    public ControlledIdentifierMember getMember() {
        return member;
    }

    public String logState() {
        return choam.logState();
    }

    public void start() {
        choam.start();
    }

    public void stop() {
        choam.stop();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getIdentifier() + "]";
    }

    // Provide the list of transactions establishing the unified KERL of the group
    private List<Transaction> genesisOf(Map<Member, Join> members) {
        log.info("Genesis joins: {} on: {}", members.keySet().stream().map(m -> m.getId()).toList(), params.member());
        var sorted = new ArrayList<Member>(members.keySet());
        sorted.sort(Comparator.naturalOrder());
        List<Transaction> transactions = new ArrayList<>();
        // Schemas
        transactions.add(transactionOf(boostrapMigration()));
        sorted.stream()
                .map(e -> manifest(members.get(e)))
                .filter(t -> t != null)
                .flatMap(l -> l.stream())
                .forEach(t -> transactions.add(t));
        transactions.add(initalMembership(params.runtime()
                .foundation()
                .getFoundation()
                .getMembershipList()
                .stream()
                .map(d -> Digest.from(d))
                .toList()));
        return transactions;
    }

    private Transaction initalMembership(List<Digest> digests) {
        var call = mutator.call("{ call apollo_kernel.add_members(?, ?) }",
                digests.stream()
                        .map(d -> new SelfAddressingIdentifier(d))
                        .map(id -> id.toIdent().toByteArray())
                        .toList(),
                "active");
        return transactionOf(Txn.newBuilder().setCall(call).build());
    }

    // Answer the KERL of this node
    private KERL_ kerl() {
        List<EventWithAttachments> kerl;
        kerl = member.getIdentifier().getKerl();
        if (kerl == null) {
            return KERL_.getDefaultInstance();
        }
        var b = KERL_.newBuilder();
        kerl.stream().map(ewa -> ewa.toKeyEvente()).forEach(ke -> b.addEvents(ke));
        return b.build();
    }

    // Manifest the transactions that instantiate the KERL for this Join. The Join
    // is validated as a side effect and if invalid, NULL is returned.
    private List<Transaction> manifest(Join join) {
        return join.getKerl().getEventsList().stream().map(ke -> transactionOf(ke)).toList();
    }

    private Transaction transactionOf(KeyEventWithAttachments ke) {
        var event = switch (ke.getEventCase()) {
            case EVENT_NOT_SET -> null;
            case INCEPTION -> ProtobufEventFactory.toKeyEvent(ke.getInception());
            case INTERACTION -> new InteractionEventImpl(ke.getInteraction());
            case ROTATION -> ProtobufEventFactory.toKeyEvent(ke.getRotation());
            default -> throw new IllegalArgumentException("Unexpected value: " + ke.getEventCase());
        };
        var batch = mutator.batch();
        batch.execute(mutator.call("{ ? = call stereotomy.append(?, ?, ?) }",
                Collections.singletonList(JDBCType.BINARY), event.getBytes(), event.getIlk(),
                DigestAlgorithm.DEFAULT.digestCode()));
        if (!ke.getAttachment().equals(Attachment.getDefaultInstance())) {
            var attach = AttachmentEvent.newBuilder()
                    .setCoordinates(event.getCoordinates().toEventCoords())
                    .setAttachment(ke.getAttachment())
                    .build();
            batch.execute(mutator.call("{ ? = call stereotomy.appendAttachment(?) }",
                    Collections.singletonList(JDBCType.BINARY), attach.toByteArray()));
        }
        return transactionOf(Txn.newBuilder().setBatched(batch.build()).build());
    }

    private Transaction transactionOf(Message message) {
        ByteBuffer buff = ByteBuffer.allocate(4);
        buff.putInt(0);
        buff.flip();
        var signer = new Signer.MockSigner(params.viewSigAlgorithm());
        var digeste = params.digestAlgorithm().getOrigin().toDigeste();
        var sig = signer.sign(digeste.toByteString().asReadOnlyByteBuffer(), buff,
                message.toByteString().asReadOnlyByteBuffer());
        return Transaction.newBuilder()
                .setSource(digeste)
                .setContent(message.toByteString())
                .setSignature(sig.toSig())
                .build();
    }

    public record TransactionConfiguration(Executor executor, ScheduledExecutorService scheduler) {
    }
}
