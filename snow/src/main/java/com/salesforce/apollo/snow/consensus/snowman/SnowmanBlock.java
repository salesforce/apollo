/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowman;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import com.salesforce.apollo.snow.choices.Status;
import com.salesforce.apollo.snow.consensus.snowball.tree.Tree;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class SnowmanBlock {
    private Block                                                   blk;
    private final Map<ID, Block>                                    children = new HashMap<>();
    private com.salesforce.apollo.snow.consensus.snowball.Consensus sb;
    private boolean                                                 shouldFalter;
    private final Consensus                                         sm;

    public SnowmanBlock(Consensus sm) {
        this.sm = sm;
    }
    public SnowmanBlock(Consensus sm, Block block) {
        this(sm);
        this.blk = block;
    }

    public boolean accepted() {
        // if the block is nil, then this is the genesis which is defined as
        // accepted
        if (blk == null) {
            return true;
        }
        return blk.status() == Status.ACCEPTED;
    }

    public void addChild(Block child) {
        ID childId = child.id();
        if (getSb() == null) {
            sb = new Tree(sm.parameters(), childId);
        } else {
            sb.add(childId);
        }
        children.put(childId, child);
    }

    public com.salesforce.apollo.snow.consensus.snowball.Consensus getSb() {
        return sb;
    }

    public Block getChild(ID id) {
        return children.get(id);
    }

    public void forEachChild(BiConsumer<ID, Block> consumer) {
        children.forEach(consumer);
    }

    public Map<ID, Block> getChildren() {
        // TODO Auto-generated method stub
        return children;
    }
    public Block getBlock() { 
        return blk;
    }
    public void setShouldFalter(boolean b) {
        shouldFalter = b;
    }
    public boolean isShouldFalter() { 
        return shouldFalter;
    }

}
