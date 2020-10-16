/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.log;

import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Header {
    private final long    blockNumber;
    private final byte[]  hashResults;
    private final byte[]  hashTransactions;
    private final HashKey lastBlock;
    private final long    lastCheckpoint;
    private final long    lastReconfig;

    public Header(long blockNumber, long lastReconfig, long lastCheckpoint, byte[] hashTransactions, byte[] hashResults,
            HashKey lastBlock) {
        this.blockNumber = blockNumber;
        this.lastReconfig = lastReconfig;
        this.lastCheckpoint = lastCheckpoint;
        this.hashTransactions = hashTransactions;
        this.hashResults = hashResults;
        this.lastBlock = lastBlock;
    }

    public long getBlockNumber() {
        return blockNumber;
    }

    public byte[] getHashResults() {
        return hashResults;
    }

    public byte[] getHashTransactions() {
        return hashTransactions;
    }

    public HashKey getLastBlock() {
        return lastBlock;
    }

    public long getLastCheckpoint() {
        return lastCheckpoint;
    }

    public long getLastReconfig() {
        return lastReconfig;
    }

}
