/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Body;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Header;
import com.salesforce.apollo.consortium.support.HashedCertifiedBlock;

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
        genesis = new HashedCertifiedBlock(
                CertifiedBlock.newBuilder()
                              .setBlock(Block.newBuilder()
                                             .setHeader(Header.newBuilder().setHeight(0))
                                             .setBody(Body.newBuilder().setType(BodyType.GENESIS))
                                             .build())
                              .build());
        store.put(genesis.hash, genesis.block);
        lastBlock = lastView = checkpoint = genesis;
        return this;
    }

    public HashedCertifiedBlock getAnchor() {
        return anchor;
    }

    public HashedCertifiedBlock getGenesis() {
        return genesis;
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
        lastBlock = new HashedCertifiedBlock(
                CertifiedBlock.newBuilder()
                              .setBlock(CollaboratorContext.generateBlock(checkpoint, lastBlock.height()
                                      + 1, lastBlock.hash.bytes(), CollaboratorContext.body(BodyType.CHECKPOINT, CollaboratorContext.checkpoint(lastBlock.height() + 1, null, 0)), lastView))
                              .build());
        store.put(lastBlock.hash, lastBlock.block);
        return lastBlock;
    }

    private HashedCertifiedBlock reconfigureBlock() {
        lastBlock = new HashedCertifiedBlock(
                CertifiedBlock.newBuilder()
                              .setBlock(Block.newBuilder()
                                             .setHeader(Header.newBuilder()
                                                              .setHeight(lastBlock.height() + 1)
                                                              .setLastCheckpoint(checkpoint.height())
                                                              .setLastReconfig(lastView.height())
                                                              .setPrevious(lastBlock.hash.toByteString()))
                                             .setBody(Body.newBuilder().setType(BodyType.RECONFIGURE))
                                             .build())
                              .build());
        store.put(lastBlock.hash, lastBlock.block);
        return lastBlock;
    }

    private HashedCertifiedBlock userBlock() {
        HashedCertifiedBlock block = new HashedCertifiedBlock(
                CertifiedBlock.newBuilder()
                              .setBlock(Block.newBuilder()
                                             .setHeader(Header.newBuilder()
                                                              .setHeight(lastBlock.height() + 1)
                                                              .setLastCheckpoint(checkpoint.height())
                                                              .setLastReconfig(lastView.height())
                                                              .setPrevious(lastBlock.hash.toByteString()))
                                             .setBody(Body.newBuilder().setType(BodyType.USER))
                                             .build())
                              .build());
        store.put(block.hash, block.block);
        lastBlock = block;
        return lastBlock;
    }
}
