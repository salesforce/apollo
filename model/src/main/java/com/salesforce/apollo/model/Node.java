/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.model.schema.tables.Member.MEMBER;
import static com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
import static java.nio.file.Path.of;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.JDBCType;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Migration;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters.Builder;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.model.delphinius.ShardedOracle;
import com.salesforce.apollo.model.stereotomy.ShardedKERL;
import com.salesforce.apollo.state.Mutator;
import com.salesforce.apollo.state.SqlStateMachine;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.event.protobuf.AttachmentEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.InteractionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;
import com.salesforce.apollo.stereotomy.services.ProtoResolverService;
import com.salesforce.apollo.stereotomy.services.ProtoResolverService.BinderService;

/**
 * @author hal.hildebrand
 *
 */
public class Node {

    public enum MemberState {
        ACTIVE, JOINING, LEAVING, OFFLINE;

        public String toColumn() {
            return name().toLowerCase();
        }
    }

    private class Listener<T extends Member> implements MembershipListener<T> {
        @Override
        public void active(T member) {
            memberActive(member);
        }

        @Override
        public void offline(T member) {
            memberOffline(member);
        }
    }

    private class ProtoBinder implements BinderService {

        @Override
        public CompletableFuture<Boolean> append(KeyEvent ke) {
            var event = switch (ke.getEventCase()) {
            case EVENT_NOT_SET -> null;
            case INCEPTION -> ProtobufEventFactory.toKeyEvent(ke.getInception());
            case INTERACTION -> new InteractionEventImpl(ke.getInteraction());
            case ROTATION -> ProtobufEventFactory.toKeyEvent(ke.getRotation());
            default -> null;
            };
            var completed = new CompletableFuture<Boolean>();
            if (event == null) {
                completed.complete(false);
            } else {
                completed.complete(true);
            }
            return completed;
        }

        @Override
        public CompletableFuture<Boolean> bind(Binding binding) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public CompletableFuture<Boolean> publish(com.salesfoce.apollo.stereotomy.event.proto.KERL kerl) {
            var events = new ArrayList<com.salesforce.apollo.stereotomy.event.KeyEvent>();
            var attachments = new ArrayList<com.salesforce.apollo.stereotomy.event.AttachmentEvent>();
            kerl.getEventsList().stream().forEach(ke -> {
                var event = switch (ke.getEventCase()) {
                case EVENT_NOT_SET -> null;
                case INCEPTION -> ProtobufEventFactory.toKeyEvent(ke.getInception());
                case INTERACTION -> new InteractionEventImpl(ke.getInteraction());
                case ROTATION -> ProtobufEventFactory.toKeyEvent(ke.getRotation());
                default -> null;
                };
                if (event != null) {
                    events.add(event);
                }
                if (ke.hasAttachment()) {
                    var builder = com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent.newBuilder();
                    builder.setAttachment(ke.getAttachment()).setCoordinates(event.getCoordinates().toEventCoords());
                    attachments.add(new AttachmentEventImpl(builder.build()));
                }
            });
            var completed = new CompletableFuture<Boolean>();
            completed.complete(!events.isEmpty());
            return completed;
        }

        @Override
        public CompletableFuture<Boolean> unbind(Ident identifier) {
            // TODO Auto-generated method stub
            return null;
        }

    }

    private class ProtoResolver implements ProtoResolverService {

        @Override
        public Optional<com.salesfoce.apollo.stereotomy.event.proto.KERL> kerl(Ident prefix) {
            return commonKERL.kerl(Identifier.from(prefix)).map(kerl -> kerl(kerl));
        }

        @Override
        public Optional<Binding> lookup(Ident prefix) {
            return Optional.empty();
        }

        @Override
        public Optional<com.salesfoce.apollo.stereotomy.event.proto.KeyState> resolve(EventCoords coordinates) {
            return commonKERL.getKeyState(EventCoordinates.from(coordinates)).map(ks -> ks.toKeyState());
        }

        @Override
        public Optional<com.salesfoce.apollo.stereotomy.event.proto.KeyState> resolve(Ident prefix) {
            return commonKERL.getKeyState(Identifier.from(prefix)).map(ks -> ks.toKeyState());
        }

        private com.salesfoce.apollo.stereotomy.event.proto.KERL kerl(List<EventWithAttachments> kerl) {
            var builder = com.salesfoce.apollo.stereotomy.event.proto.KERL.newBuilder();
            kerl.forEach(ewa -> builder.addEvents(ewa.toKeyEvente()));
            return builder.build();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Node.class);

    public static void addMembers(Connection connection, List<byte[]> members) {
        var context = DSL.using(connection, SQLDialect.H2);
        for (var m : members) {
            context.insertInto(IDENTIFIER, IDENTIFIER.PREFIX).values(m).onDuplicateKeyIgnore().execute();
            var id = context.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(m)).fetchOne();
            context.insertInto(MEMBER).set(MEMBER.IDENTIFIER, id.value1()).onConflictDoNothing().execute();
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

    private static URL res(String resource) {
        return Node.class.getResource(resource);
    }

    private static Path tempDirOf(ControlledIdentifier<SelfAddressingIdentifier> id) {
        Path dir;
        try {
            dir = Files.createTempDirectory(id.getDigest().toString());
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create temporary directory", e);
        }
        dir.toFile().deleteOnExit();
        return dir;
    }

    private final CHOAM                                          choam;
    private final KERL                                           commonKERL;
    private final ControlledIdentifier<SelfAddressingIdentifier> identifier;
    private final Mutator                                        mutator;
    private final Oracle                                         oracle;
    @SuppressWarnings("unused")
    private final Context<? extends Member>                      overlay;
    private final Parameters                                     params;
    private final SqlStateMachine                                sqlStateMachine;

    public Node(Context<? extends Member> overlay, ControlledIdentifier<SelfAddressingIdentifier> id,
                Parameters.Builder params, Builder runtime) {
        this(overlay, id, params, "jdbc:h2:mem:", tempDirOf(id), runtime);
    }

    public Node(Context<? extends Member> overlay, ControlledIdentifier<SelfAddressingIdentifier> id,
                Parameters.Builder params, Path checkpointBaseDir, RuntimeParameters.Builder runtime) {
        this(overlay, id, params, "jdbc:h2:mem:", checkpointBaseDir, runtime);
    }

    public Node(Context<? extends Member> overlay, ControlledIdentifier<SelfAddressingIdentifier> id,
                Parameters.Builder params, String dbURL, Path checkpointBaseDir, RuntimeParameters.Builder runtime) {
        this.overlay = overlay;
        params = params.clone();
        var dir = checkpointBaseDir.toFile();
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IllegalArgumentException("Cannot create checkpoint base directory: " + checkpointBaseDir);
            }
        }
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("Must be a directory: " + checkpointBaseDir);
        }
        var checkpointDir = new File(dir, qb64(((SelfAddressingIdentifier) id.getIdentifier()).getDigest()));
        this.identifier = id;
        sqlStateMachine = new SqlStateMachine(dbURL, new Properties(), checkpointDir);

        this.params = params.build(runtime.setCheckpointer(sqlStateMachine.getCheckpointer())
                                          .setProcessor(sqlStateMachine.getExecutor())
                                          .setRestorer(sqlStateMachine.getBootstrapper())
                                          .setKerl(() -> kerl())
                                          .setGenesisData(members -> genesisOf(members))
                                          .build());
        choam = new CHOAM(this.params);
        mutator = sqlStateMachine.getMutator(choam.getSession());
        this.oracle = new ShardedOracle(sqlStateMachine.newConnection(), mutator, runtime.getScheduler(),
                                        params.getSubmitTimeout(), runtime.getExec());
        this.commonKERL = new ShardedKERL(sqlStateMachine.newConnection(), mutator, runtime.getScheduler(),
                                          params.getSubmitTimeout(), params.getDigestAlgorithm(), runtime.getExec());
        overlay.register(new Listener<>());
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
        return identifier.getIdentifier();
    }

    public SigningMember getMember() {
        return params.member();
    }

    /**
     * @return the BinderService that provides raw Protobuf bindings
     */
    public ProtoResolverService.BinderService getProtoBinder() {
        return new ProtoBinder();
    }

    /**
     * @return the ResolverService that provides raw Protobuf access to the
     *         underlying KERI resolution
     */
    public ProtoResolverService getProtoResolver() {
        return new ProtoResolver();
    }

    public Optional<CertificateWithPrivateKey> provision(com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment validators,
                                                         InetSocketAddress endpoint, Duration duration,
                                                         SignatureAlgorithm signatureAlgorithm) {
        return identifier.provision(validators, endpoint, Instant.now(), duration, signatureAlgorithm);
    }

    public void start() {
        choam.start();
    }

    public void stop() {
        choam.stop();
    }

    @Override
    public String toString() {
        return "Node[" + getIdentifier() + "]";
    }

    // Provide the list of transactions establishing the unified KERL of the group
    private List<Transaction> genesisOf(Map<Member, Join> members) {
        log.info("Genesis joins: {} on: {}", members.keySet(), params.member());
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
        var call = mutator.call("{ call apollo_kernel.add_members(?) }",
                                digests.stream()
                                       .map(d -> new SelfAddressingIdentifier(d))
                                       .map(id -> id.toIdent().toByteArray())
                                       .toList());
        return transactionOf(Txn.newBuilder().setCall(call).build());
    }

    // Answer the KERL of this node
    private com.salesfoce.apollo.stereotomy.event.proto.KERL kerl() {
        var kerl = identifier.getKerl();
        if (kerl.isEmpty()) {
            return com.salesfoce.apollo.stereotomy.event.proto.KERL.getDefaultInstance();
        }
        var b = com.salesfoce.apollo.stereotomy.event.proto.KERL.newBuilder();
        kerl.get().stream().map(ewa -> ewa.toKeyEvente()).forEach(ke -> b.addEvents(ke));
        return b.build();
    }

    // Manifest the transactions that instantiate the KERL for this Join. The Join
    // is validated as a side effect and if invalid, NULL is returned.
    private List<Transaction> manifest(Join join) {
        return join.getKerl().getEventsList().stream().map(ke -> transactionOf(ke)).toList();
    }

    private void memberActive(Member member) {
        log.trace("Member active: {} on: {}", member, params.member());
        params.context().activate(member);
    }

    private void memberOffline(Member member) {
        log.trace("Member offline: {} on: {}", member, params.member());
        params.context().offline(member);
    }

    private Transaction transactionOf(KeyEvent ke) {
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
}
