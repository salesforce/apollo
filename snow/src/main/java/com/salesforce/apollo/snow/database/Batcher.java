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
public interface Batcher {
    // NewBatch creates a write-only database that buffers changes to its host db
    // until a final write is called.
    Batch newBatch();
}
