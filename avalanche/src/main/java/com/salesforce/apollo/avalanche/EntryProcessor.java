/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.protocols.HashKey;

/**
 * A block processor for Avalanche.
 * 
 * @author hhildebrand
 */
@FunctionalInterface
public interface EntryProcessor {
    /**
     * Validate the block. Answer the hash of the DAG node that represents the conflict set for the input block.
     * 
     * @param block
     * @return the DAG block conflict set, null if invalid
     */
    HashKey validate(HashKey hash, DagEntry block);
}
