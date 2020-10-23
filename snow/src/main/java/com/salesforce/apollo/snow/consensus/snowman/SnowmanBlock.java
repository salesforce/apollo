/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowman;

import java.util.HashMap;
import java.util.Map;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class SnowmanBlock {
    protected Consensus                                               sm;
    protected Block                                                   blk;
    protected boolean                                                 shouldFalter;
    protected com.salesforce.apollo.snow.consensus.snowball.Consensus sb;
    protected final Map<ID, Block>                                    chidren = new HashMap<>();
    
    public void addChild(Block child) {
        ID childId = child.id(); 
    }

}
