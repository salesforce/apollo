/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball.tree;

import com.salesforce.apollo.snow.consensus.snowball.UnarySnowball;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class UnaryNode extends Node<UnarySnowball> {

    public UnaryNode(Tree tree, ID choice, int numbits, UnarySnowball snowball) {
        super(tree, choice, numbits, snowball);
    }
}
