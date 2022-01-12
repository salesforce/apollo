/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import static java.nio.file.Path.of;

import java.net.URL;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.salesfoce.apollo.state.proto.Migration;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.delphinius.Oracle;
import com.salesforce.apollo.model.delphinius.ShardedOracle;
import com.salesforce.apollo.state.Mutator;
import com.salesforce.apollo.stereotomy.ControlledIdentifier;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
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

    private final KERL                 commonKERL;
    private final ControlledIdentifier id;
    private final Shard                shard;
    private final Oracle               oracle;

    public Node(ControlledIdentifier id, Shard shard, KERL commonKERL, DigestAlgorithm digestAlgorithm,
                SecureRandom entropy) throws SQLException {
        this.id = id;
        this.shard = shard;
        this.commonKERL = commonKERL;
        this.oracle = new ShardedOracle(shard.createConnection(), shard.getMutator(), null, null, null);
    }

    public Identifier getId() {
        return id.getIdentifier();
    }
}
