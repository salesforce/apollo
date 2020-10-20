/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Proof;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
abstract public class Committee {

    protected final Map<Long, Block> cache               = new ConcurrentHashMap<>();
    protected volatile HashKey       lastBlock           = HashKey.ORIGIN;
    protected volatile long          lastCheckpoint      = 1;
    protected volatile long          lastReconfiguration = 1;
    protected volatile StateSnapshot lastSnapshot        = new StateSnapshot();
    protected volatile long          next                = 1;

    void initialize() {
        lastBlock = HashKey.ORIGIN;
        lastCheckpoint = 1;
        lastReconfiguration = 1;
        lastSnapshot = new StateSnapshot();
        next = 1;
        resetCached();
        writeGenesisBlock();
    }

    void resetCached() {
        cache.clear();
    }

    void writeGenesisBlock() {
        // TODO Auto-generated method stub

    }

    void deliverTranssactions(long consensusId, List<Transaction> transactions, List<Proof> proofs) {

    }
}
