/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.CertifiedBlock;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Genesis;
import com.salesfoce.apollo.choam.proto.Header;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.Store;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public class TestChain {

    private HashedCertifiedBlock anchor;
    private HashedCertifiedBlock checkpoint;
    private HashedCertifiedBlock genesis;
    private HashedCertifiedBlock lastBlock;
    private HashedCertifiedBlock lastView;
    private final Store          store;
    private HashedCertifiedBlock synchronizeCheckpoint;
    private HashedCertifiedBlock synchronizeView;

    public TestChain(Store store) {
        this.store = store;
    }

    public TestChain anchor() {
        anchor = lastBlock;
        return this;
    }

    public TestChain checkpoint() {
        checkpoint = lastBlock = checkpointBlock();
        return this;
    }

    public TestChain genesis() {
        genesis = new HashedCertifiedBlock(DigestAlgorithm.DEFAULT,
                                           CertifiedBlock.newBuilder()
                                                         .setBlock(Block.newBuilder()
                                                                        .setHeader(Header.newBuilder()
                                                                                         .setHeight(0)
                                                                                         .setLastCheckpoint(-1)
                                                                                         .setLastReconfig(-1))
                                                                        .setGenesis(Genesis.getDefaultInstance())
                                                                        .build())
                                                         .build());
        store.put(genesis);
        lastBlock = lastView = checkpoint = genesis;
        return this;
    }

    public HashedCertifiedBlock getAnchor() {
        return anchor;
    }

    public HashedCertifiedBlock getGenesis() {
        return genesis;
    }

    public HashedCertifiedBlock getLastBlock() {
        return lastBlock;
    }

    public HashedCertifiedBlock getSynchronizeCheckpoint() {
        return synchronizeCheckpoint;
    }

    public HashedCertifiedBlock getSynchronizeView() {
        return synchronizeView;
    }

    public TestChain synchronizeCheckpoint() {
        synchronizeCheckpoint = checkpoint = lastBlock = checkpointBlock();
        return this;
    }

    public TestChain synchronizeView() {
        synchronizeView = lastView = lastBlock = reconfigureBlock();
        return this;
    }

    public TestChain userBlocks(int count) {
        for (int i = 0; i < count; i++) {
            lastBlock = userBlock();
        }
        return this;
    }

    public TestChain viewChange() {
        lastView = lastBlock = reconfigureBlock();
        return this;
    }

    private HashedCertifiedBlock checkpointBlock() {
        lastBlock = new HashedCertifiedBlock(DigestAlgorithm.DEFAULT,
                                             CertifiedBlock.newBuilder()
                                                           .setBlock(Block.newBuilder()
                                                                          .setHeader(Header.newBuilder()
                                                                                           .setLastCheckpoint(checkpoint.height()
                                                                                                                        .longValue())
                                                                                           .setLastCheckpointHash(checkpoint.hash.toDigeste())
                                                                                           .setLastReconfig(lastView.height()
                                                                                                                    .longValue())
                                                                                           .setLastReconfigHash(lastView.hash.toDigeste())
                                                                                           .setHeight(lastBlock.height()
                                                                                                               .add(1)
                                                                                                               .longValue())
                                                                                           .setPrevious(lastBlock.hash.toDigeste()))
                                                                          .setCheckpoint(CHOAM.checkpoint(DigestAlgorithm.DEFAULT,
                                                                                                          null, 0))
                                                                          .build())
                                                           .build());
        store.put(lastBlock);
        return lastBlock;
    }

    private HashedCertifiedBlock reconfigureBlock() {
        lastBlock = new HashedCertifiedBlock(DigestAlgorithm.DEFAULT,
                                             CertifiedBlock.newBuilder()
                                                           .setBlock(Block.newBuilder()
                                                                          .setHeader(Header.newBuilder()
                                                                                           .setLastCheckpoint(checkpoint.height()
                                                                                                                        .longValue())
                                                                                           .setLastCheckpointHash(checkpoint.hash.toDigeste())
                                                                                           .setLastReconfig(lastView.height()
                                                                                                                    .longValue())
                                                                                           .setLastReconfigHash(lastView.hash.toDigeste())
                                                                                           .setHeight(lastBlock.height()
                                                                                                               .add(1)
                                                                                                               .longValue())
                                                                                           .setPrevious(lastBlock.hash.toDigeste()))
                                                                          .setReconfigure(Reconfigure.getDefaultInstance())
                                                                          .build())
                                                           .build());
        store.put(lastBlock);
        return lastBlock;
    }

    private HashedCertifiedBlock userBlock() {
        HashedCertifiedBlock block = new HashedCertifiedBlock(DigestAlgorithm.DEFAULT,
                                                              CertifiedBlock.newBuilder()
                                                                            .setBlock(Block.newBuilder()
                                                                                           .setHeader(Header.newBuilder()
                                                                                                            .setLastCheckpoint(checkpoint.height()
                                                                                                                                         .longValue())
                                                                                                            .setLastCheckpointHash(checkpoint.hash.toDigeste())
                                                                                                            .setLastReconfig(lastView.height()
                                                                                                                                     .longValue())
                                                                                                            .setLastReconfigHash(lastView.hash.toDigeste())
                                                                                                            .setHeight(lastBlock.height()
                                                                                                                                .add(1)
                                                                                                                                .longValue())
                                                                                                            .setPrevious(lastBlock.hash.toDigeste()))
                                                                                           .setExecutions(Executions.getDefaultInstance())
                                                                                           .build())
                                                                            .build());
        store.put(block);
        lastBlock = block;
        return lastBlock;
    }
}
