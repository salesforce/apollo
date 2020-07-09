/*
s * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.chain.materialized;

import com.salesforce.apollo.chain.Block;
import com.salesforce.apollo.chain.MutableChain;

/**
 * 
 * @author hal.hildebrand
 *
 * @param <M> - the type of values in the view
 */
public abstract class MutableView<M> extends View<M> {

    public MutableView(MutableChain<M> chain) {
        super(chain);
    }

    public MutableChain<M> getChain() {
        return (MutableChain<M>) chain;
    }

    public abstract M put(Block<M> value);

}
