/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.security.SecureRandom;
import java.sql.SQLException;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.model.stereotomy.ShardedKERL;
import com.salesforce.apollo.stereotomy.Stereotomy;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.StereotomyKeyStore;

/**
 * @author hal.hildebrand
 *
 */
public class Node {

    private final Digest     id;
    @SuppressWarnings("unused")
    private final Shard      shard;
    @SuppressWarnings("unused")
    private final Stereotomy controller;

    public Node(Digest id, Shard shard, StereotomyKeyStore keyStore, DigestAlgorithm digestAlgorithm,
                SecureRandom entropy) throws SQLException {
        this.id = id;
        this.shard = shard;
        this.controller = new StereotomyImpl(keyStore, new ShardedKERL(shard.createConnection(), shard.getMutator(),
                                                                       null, null, digestAlgorithm, null),
                                             entropy);
    }

    public Digest getId() {
        return id;
    }
}
