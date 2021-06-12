/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.util.Iterator;

import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.crypto.Digest;

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

    public DagEntry get(Digest key) {
        return dag.getDagEntry(key);
    }

    public Boolean isFinalized(Digest hash) {
        return dag.isFinalized(hash);
    }

    public Iterator<Digest> allFinalized() {
        return dag.allFinalized();
    }

}
