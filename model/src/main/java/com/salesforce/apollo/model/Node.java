/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static java.nio.file.Path.of;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Migration;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.model.delphinius.ShardedOracle;
import com.salesforce.apollo.model.stereotomy.ShardedKERL;
import com.salesforce.apollo.state.Mutator;
import com.salesforce.apollo.state.SqlStateMachine;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.event.protobuf.InteractionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public class Node {
    private static final Logger log = LoggerFactory.getLogger(Node.class);

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
    @SuppressWarnings("unused")
    private final KERL                                           commonKERL;
    private final ControlledIdentifier<SelfAddressingIdentifier> identifier;
    private final Mutator                                        mutator;
    @SuppressWarnings("unused")
    private final Oracle                                         oracle;
    private final Parameters                                     params;
    private final SqlStateMachine                                sqlStateMachine;

    public Node(ControlledIdentifier<SelfAddressingIdentifier> id, Parameters.Builder params) {
        this(id, params, "jdbc:h2:mem:", tempDirOf(id));
    }

    public Node(ControlledIdentifier<SelfAddressingIdentifier> id, Parameters.Builder params, Path checkpointBaseDir) {
        this(id, params, "jdbc:h2:mem:", checkpointBaseDir);
    }

    public Node(ControlledIdentifier<SelfAddressingIdentifier> id, Parameters.Builder params, String dbURL,
                Path checkpointBaseDir) {
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
        params.setCheckpointer(sqlStateMachine.getCheckpointer());
        params.setProcessor(sqlStateMachine.getExecutor());
        params.setRestorer(sqlStateMachine.getBootstrapper());
        params.setKerl(() -> kerl());
        params.setGenesisData(members -> genesisOf(members));

        this.params = params.build();
        choam = new CHOAM(this.params);
        mutator = sqlStateMachine.getMutator(choam.getSession());
        this.oracle = new ShardedOracle(sqlStateMachine.newConnection(), mutator, params.getScheduler(),
                                        params.getSubmitTimeout(), params.getExec());
        this.commonKERL = new ShardedKERL(sqlStateMachine.newConnection(), mutator, params.getScheduler(),
                                          params.getSubmitTimeout(), params.getDigestAlgorithm(), params.getExec());
    }

    /**
     * @return the Identifier of the receiver
     */
    public Identifier getIdentifier() {
        return identifier.getIdentifier();
    }

    public void start() {
        choam.start();
    }

    public void stop() {
        choam.stop();
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
        return transactions;
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
