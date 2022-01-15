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
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Path;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.salesfoce.apollo.choam.proto.Join;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Migration;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Session;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.model.delphinius.ShardedOracle;
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

    @SuppressWarnings("unused")
    private final KERL                                           commonKERL;
    private final ControlledIdentifier<SelfAddressingIdentifier> identifier;
    @SuppressWarnings("unused")
    private final Oracle                                         oracle;
    private final Parameters                                     params;
    private final Shard                                          shard;

    public Node(ControlledIdentifier<SelfAddressingIdentifier> id, Parameters.Builder params, KERL commonKERL,
                String dbURL, Path checkpointBaseDir) throws SQLException {
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
        this.commonKERL = commonKERL;
        SqlStateMachine sqlStateMachine = new SqlStateMachine(dbURL, new Properties(), checkpointDir);
        // TODO for now
        var cert = id.provision(InetSocketAddress.createUnresolved("localhost", 0), Instant.now(), Duration.ofHours(1),
                                SignatureAlgorithm.DEFAULT);

        params.setMember(new SigningMemberImpl(cert.get()));
        params.setCheckpointer(sqlStateMachine.getCheckpointer());
        params.setProcessor(sqlStateMachine.getExecutor());
        params.setRestorer(sqlStateMachine.getBootstrapper());
        params.setKerl(() -> kerl());
        params.setGenesisData(members -> genesisOf(members));

        this.params = params.build();
        this.shard = new CHOAMShard(new CHOAM(this.params), sqlStateMachine);
        this.oracle = new ShardedOracle(shard.createConnection(), shard.getMutator(), null, null, null);
    }

    /**
     * @return the Identifier of the receiver
     */
    public Identifier getIdentifier() {
        return identifier.getIdentifier();
    }

    // Provide the list of transactions establishing the unified KERL of the group
    private List<Transaction> genesisOf(Map<Member, Join> members) {
        return members.entrySet()
                      .stream()
                      .sorted()
                      .map(e -> manifest(e.getValue()))
                      .filter(t -> t != null)
                      .flatMap(l -> l.stream())
                      .toList();
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
        // TODO validate
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
        var mutator = shard.getMutator();
        var batch = mutator.batch();
        batch.execute(mutator.call("{ ? = call stereotomy.append(?, ?, ?) }",
                                   Collections.singletonList(JDBCType.BINARY), event.getBytes(), event.getIlk(),
                                   DigestAlgorithm.DEFAULT.digestCode()));
        if (ke.getAttachment().isInitialized()) {
            var attach = AttachmentEvent.newBuilder()
                                        .setCoordinates(event.getCoordinates().toEventCoords())
                                        .setAttachment(ke.getAttachment())
                                        .build();
            batch.execute(mutator.call("{ ? = call stereotomy.appendAttachement(?) }",
                                       Collections.singletonList(JDBCType.BINARY), attach.toByteArray()));
        }
        return Session.transactionOf(params.member().getId(), 0, batch.build(), params.member());
    }

}
