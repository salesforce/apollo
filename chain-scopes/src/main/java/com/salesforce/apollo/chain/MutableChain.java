/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.chain;

/**
 * @author hal.hildebrand
 *
 */
public interface MutableChain<M> extends Chain<M> {

    void insert(Block<M> block);

}
