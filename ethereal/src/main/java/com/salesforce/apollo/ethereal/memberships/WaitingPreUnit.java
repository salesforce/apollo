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

class WaitingPreUnit {
    private final List<WaitingPreUnit> children       = new ArrayList<>();
    private volatile boolean           failed         = false;
    private volatile int               missingParents = 0;
    private final PreUnit              pu;
    private final PreUnit_s            serialized;
    private volatile int               waitingParents = 0;

    WaitingPreUnit(PreUnit pu) {
        this(pu, pu.toPreUnit_s());
    }

    WaitingPreUnit(PreUnit pu, PreUnit_s serialized) {
        this.pu = pu;
        this.serialized = serialized;
    }

    public void addChild(WaitingPreUnit wp) {
        children.add(wp);
    }

    public List<WaitingPreUnit> children() {
        return children;
    }

    public void clearAndAdd(List<WaitingPreUnit> c) {
        children.clear();
        children.addAll(c);
    }

    public void clearChildren() {
        children.clear();
    }

    public void decMissing() {
        final var m = missingParents;
        missingParents = m - 1;
    }

    public void decWaiting() {
        final var w = waitingParents;
        waitingParents = w - 1;
    }

    public int epoch() {
        return pu.epoch();
    }

    public void fail() {
        failed = true;
    }

    public boolean failed() {
        return failed;
    }

    public Digest hash() {
        return pu.hash();
    }

    public Long id() {
        return pu.id();
    }

    public void incMissing() {
        final var m = missingParents;
        missingParents = m + 1;
    }

    public void incWaiting() {
        final var w = waitingParents;
        waitingParents = w + 1;
    }

    public int missingParents() {
        return missingParents;
    }

    public boolean parentsOutput() {
        final int cMissingParents = missingParents;
        final int cWaitingParents = waitingParents;
        return cWaitingParents == 0 && cMissingParents == 0;
    }

    public PreUnit pu() {
        return pu;
    }

    public PreUnit_s serialized() {
        return serialized;
    }

    @Override
    public String toString() {
        return "wpu[" + pu.shortString() + "]";
    }
}
