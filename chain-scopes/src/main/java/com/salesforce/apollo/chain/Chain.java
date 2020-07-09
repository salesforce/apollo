/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.chain;

import java.util.function.Consumer;

import com.salesforce.apollo.avro.HASH;

/**
 * 
 * @author hal.hildebrand
 * 
 *         A linear chain o' blocks
 *
 */
public interface Chain<M> {

    HASH getKey();

    Block<M> getBlock(HASH index);

    void onAccept(Consumer<Block<M>> consumer);
}
