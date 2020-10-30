/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.database;

/**
 * @author hal.hildebrand
 *
 */
public interface Compactor {
    // Compact the underlying DB for the given key range.
    // Specifically, deleted and overwritten versions are discarded,
    // and the data is rearranged to reduce the cost of operations
    // needed to access the data. This operation should typically only
    // be invoked by users who understand the underlying implementation.
    //
    // A nil start is treated as a key before all keys in the DB.
    // And a nil limit is treated as a key after all keys in the DB.
    // Therefore if both are nil then it will compact entire DB.
    void compact(byte[] start, byte[] limit);
}
