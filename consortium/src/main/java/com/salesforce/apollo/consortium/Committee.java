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

import com.salesforce.apollo.consortium.log.Block;
import com.salesforce.apollo.consortium.log.Proof;
import com.salesforce.apollo.consortium.log.Transaction;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Committee {

    private final Map<Long, Block> cache               = new ConcurrentHashMap<>();
    private final Context<Member>  context;
    private final HashKey          id;
    private HashKey                lastBlock           = HashKey.ORIGIN;
    private long                   lastCheckpoint      = 1;
    private long                   lastReconfiguration = 1;
    private StateSnapshot          lastSnapshot        = new StateSnapshot();
    private long                   next                = 1;

    public Committee(HashKey id, Context<Member> context) {
        this.context = context;
        this.id = null;
    }

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

    }

    void writeGenesisBlock() {
        // TODO Auto-generated method stub

    }

    void deliverTranssactions(long consensusId, List<Transaction> transactions, List<Proof> proofs) {

    }
}
