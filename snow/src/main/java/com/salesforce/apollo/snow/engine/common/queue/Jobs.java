/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common.queue;

import com.salesforce.apollo.snow.database.Database;

/**
 * @author hal.hildebrand
 *
 */
public class Jobs {
    private Parser parser;
    private Database baseDB;
    
    // Dynamic sized stack of ready to execute items
    // Map from itemID to list of itemIDs that are blocked on this item
    prefixedState state;
}
