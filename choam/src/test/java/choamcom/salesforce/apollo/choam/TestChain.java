/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package choamcom.salesforce.apollo.choam;

import com.salesfoce.apollo.choam.proto.Block;
import com.salesfoce.apollo.choam.proto.Executions;
import com.salesfoce.apollo.choam.proto.Genesis;
import com.salesfoce.apollo.choam.proto.Header;
import com.salesfoce.apollo.choam.proto.Reconfigure;
import com.salesforce.apollo.choam.support.HashedBlock;
import com.salesforce.apollo.choam.support.Store;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public class TestChain {

    private HashedBlock anchor;
    private HashedBlock checkpoint;
    private HashedBlock genesis;
    private HashedBlock lastBlock;
    private HashedBlock lastView;
    private final Store store;
    private HashedBlock synchronizeCheckpoint;
    private HashedBlock synchronizeView;

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
        genesis = new HashedBlock(DigestAlgorithm.DEFAULT,
                                  Block.newBuilder()
                                       .setHeader(Header.newBuilder().setHeight(0).setLastCheckpoint(-1)
                                                        .setLastReconfig(-1))
                                       .setGenesis(Genesis.getDefaultInstance()).build());
        store.put(genesis);
        lastBlock = lastView = checkpoint = genesis;
        return this;
    }

    public HashedBlock getAnchor() {
        return anchor;
    }

    public HashedBlock getGenesis() {
        return genesis;
    }

    public HashedBlock getLastBlock() {
        return lastBlock;
    }

    public HashedBlock getSynchronizeCheckpoint() {
        return synchronizeCheckpoint;
    }

    public HashedBlock getSynchronizeView() {
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

    private HashedBlock checkpointBlock() {
        lastBlock = new HashedBlock(DigestAlgorithm.DEFAULT,
                                    Block.newBuilder()
                                         .setHeader(Header.newBuilder().setLastCheckpoint(checkpoint.height())
                                                          .setLastCheckpointHash(checkpoint.hash.toDigeste())
                                                          .setLastReconfig(lastView.height())
                                                          .setLastReconfigHash(lastView.hash.toDigeste())
                                                          .setHeight(lastBlock.height() + 1)
                                                          .setPrevious(lastBlock.hash.toDigeste()))
                                         .setCheckpoint(HashedBlock.checkpoint(DigestAlgorithm.DEFAULT, null, 0))
                                         .build());
        store.put(lastBlock);
        return lastBlock;
    }

    private HashedBlock reconfigureBlock() {
        lastBlock = new HashedBlock(DigestAlgorithm.DEFAULT,
                                    Block.newBuilder()
                                         .setHeader(Header.newBuilder().setLastCheckpoint(checkpoint.height())
                                                          .setLastCheckpointHash(checkpoint.hash.toDigeste())
                                                          .setLastReconfig(lastView.height())
                                                          .setLastReconfigHash(lastView.hash.toDigeste())
                                                          .setHeight(lastBlock.height() + 1)
                                                          .setPrevious(lastBlock.hash.toDigeste()))
                                         .setReconfigure(Reconfigure.getDefaultInstance()).build());
        store.put(lastBlock);
        return lastBlock;
    }

    private HashedBlock userBlock() {
        lastBlock = new HashedBlock(DigestAlgorithm.DEFAULT,
                                    Block.newBuilder()
                                         .setHeader(Header.newBuilder().setLastCheckpoint(checkpoint.height())
                                                          .setLastCheckpointHash(checkpoint.hash.toDigeste())
                                                          .setLastReconfig(lastView.height())
                                                          .setLastReconfigHash(lastView.hash.toDigeste())
                                                          .setHeight(lastBlock.height() + 1)
                                                          .setPrevious(lastBlock.hash.toDigeste()))
                                         .setExecutions(Executions.getDefaultInstance()).build());
        store.put(lastBlock);
        return lastBlock;
    }
}
