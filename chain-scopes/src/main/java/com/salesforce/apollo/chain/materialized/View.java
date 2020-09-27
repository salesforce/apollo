/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.chain.materialized;

import java.util.function.Consumer;
import java.util.stream.Stream;

import com.salesforce.apollo.chain.Block;
import com.salesforce.apollo.chain.Chain;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @param <M> - the type of values in the view
 *
 */
public abstract class View<M> implements Consumer<Block<M>> {
    public static class Entry<M> {
        private HashKey  key;
        private Block<M> value;

        public HashKey getKey() {
            return key;
        }

        public Block<M> getValue() {
            return value;
        }

    }

    protected final Chain<M> chain;

    public View(Chain<M> chain) {
        this.chain = chain;
        chain.onAccept(this);
    }

    public Chain<M> getChain() {
        return chain;
    }

    public Block<M> get(HashKey key) {
        return chain.getBlock(key);
    }

    public abstract Stream<Entry<M>> span(HashKey key, int start, int end);
}
