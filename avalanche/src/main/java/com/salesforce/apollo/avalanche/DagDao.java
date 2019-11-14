/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.util.List;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.protocols.HashKey;

/**
 * Restricted API on the DAG
 * 
 * @author hhildebrand
 */
public class DagDao {
    private final WorkingSet dag;

    public DagDao(WorkingSet dag) {
        this.dag = dag;
    }

    public DagEntry get(HashKey key) {
        return dag.getDagEntry(key);
    }

    public Boolean isFinalized(HashKey hash) {
        return dag.isFinalized(hash);
    }

    public List<HashKey> allFinalized() {
        return dag.allFinalized();
    }

}
