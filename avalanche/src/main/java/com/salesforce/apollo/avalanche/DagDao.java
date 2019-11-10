/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
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

    public DagEntry get(HASH key) {
        return dag.getDagEntry(new HashKey(key));
    }

    public Boolean isFinalized(HASH hash) {
        return dag.isFinalized(new HashKey(hash));
    }

}
