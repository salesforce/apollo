/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import java.util.ArrayList;
import java.util.List;

import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.PreUnit;

public class MaterializedNode implements Node {
    private enum State {
        INITIAL;
    }

    private final List<MaterializedNode>   children       = new ArrayList<>();
    private volatile boolean   failed         = false;
    @SuppressWarnings("unused")
    private final Digest       hash;
    private volatile int       missingParents = 0;
    private volatile PreUnit_s pu;
    private State              state          = State.INITIAL;
    private volatile int       waitingParents = 0;

    public MaterializedNode(Digest hash, PreUnit_s pu) {
        this.hash = hash;
        this.pu = pu;
    }

    public void fail() {
        failed = true;
        pu = null;
        children.clear();
    }

    public boolean failed() {
        return failed;
    }

    public PreUnit_s getPu() {
        return pu;
    }

    public boolean parentsOutput() {
        final int cMissingParents = missingParents;
        final int cWaitingParents = waitingParents;
        return cWaitingParents == 0 && cMissingParents == 0;
    }

    @Override
    public String toString() {
        return "node[" + PreUnit.decode(pu.getId()) + ":" + state + "]";
    }
}
