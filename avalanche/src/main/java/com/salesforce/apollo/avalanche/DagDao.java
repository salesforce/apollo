/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import org.jooq.DSLContext;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;

/**
 * Restricted API on the DAG
 * 
 * @author hhildebrand
 */
public class DagDao {
    private final Dag dag;
    private final DSLContext context;

    public DagDao(Dag dag, DSLContext context) {
        this.dag = dag;
        this.context = context;
    }

    public DagEntry get(HASH key) {
        return dag.getDagEntry(key, context);
    }

    public Boolean isFinalized(HASH hash) { 
        return dag.isFinalized(hash, context);
    }

}
