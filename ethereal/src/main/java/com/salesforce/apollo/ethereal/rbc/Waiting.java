/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.rbc;

import java.util.ArrayList;
import java.util.List;

import com.salesfoce.apollo.ethereal.proto.PreUnit_s;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.PreUnit;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.rbc.ChRbcAdder.State;

/**
 * 
 * @author hal.hildebrand
 *
 */
public class Waiting {

    private final List<Waiting> children       = new ArrayList<>();
    private volatile Unit       decoded;
    private volatile int        missingParents = 0;
    private final PreUnit       pu;
    private final PreUnit_s     serialized;
    private volatile State      state;
    private volatile int        waitingParents = 0;

    public Waiting(PreUnit pu) {
        this(pu, pu.toPreUnit_s());
    }

    public Waiting(PreUnit pu, PreUnit_s serialized) {
        this.pu = pu;
        this.serialized = serialized;
        state = State.PROPOSED;
    }

    public void addChild(Waiting wp) {
        children.add(wp);
    }

    public List<Waiting> children() {
        return children;
    }

    public void clearAndAdd(List<Waiting> c) {
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

    public Unit decoded() {
        return decoded;
    }

    public void decWaiting() {
        final var w = waitingParents;
        waitingParents = w - 1;
    }

    public int epoch() {
        return pu.epoch();
    }

    public Digest hash() {
        return pu.hash();
    }

    public int height() {
        return pu.height();
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
        final var current = missingParents;
        return current;
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

    public void setDecoded(Unit decoded) {
        this.decoded = decoded;
    }

    public void setState(State state) {
        this.state = state;
    }

    public State state() {
        final var current = state;
        return current;
    }

    @Override
    public String toString() {
        return "wpu:" + state() + ":[" + pu.shortString() + "]";
    }
}
