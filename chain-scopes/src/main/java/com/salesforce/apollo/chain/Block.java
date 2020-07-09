/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.chain;

import com.salesforce.apollo.avro.HASH;

/**
 * A hash identified block o bytes
 * 
 * @author hal.hildebrand
 *
 */
public abstract class Block<T> {

    private final byte[] content;
    private final HASH   key;
    private final HASH   parent;

    public Block(HASH parent, HASH key, byte[] content) {
        this.parent = parent;
        this.key = key;
        this.content = content;
    }

    public byte[] getContent() {
        return content;
    }

    public HASH getKey() {
        return key;
    }

    public HASH getParent() {
        return parent;
    }

    public abstract T getValue();
}
