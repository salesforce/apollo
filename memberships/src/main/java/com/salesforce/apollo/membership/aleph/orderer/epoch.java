/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph.orderer;

import static com.salesforce.apollo.membership.aleph.Adder.newAdder;
import static com.salesforce.apollo.membership.aleph.Dag.newDag;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import com.salesforce.apollo.membership.aleph.Adder;
import com.salesforce.apollo.membership.aleph.Alerter;
import com.salesforce.apollo.membership.aleph.Config;
import com.salesforce.apollo.membership.aleph.Dag;
import com.salesforce.apollo.membership.aleph.RandomSource;
import com.salesforce.apollo.membership.aleph.RandomSource.RandomSourceFactory;
import com.salesforce.apollo.membership.aleph.Syncer;
import com.salesforce.apollo.membership.aleph.Unit;
import com.salesforce.apollo.membership.aleph.linear.ExtenderService;

/**
 * @author hal.hildebrand
 *
 */
public record epoch(int id, Adder adder, Dag dag, ExtenderService extender, RandomSource rs, Queue<Boolean> more) {
    public static epoch newEpoch(int id, Config config, Syncer syncer, RandomSourceFactory rsf, Alerter alert,
                                 BlockingQueue<Unit> unitBelt, BlockingQueue<Unit> output) {
        Dag dg = newDag(config, id);
        Adder adr = newAdder(dg, config, syncer, alert);
        RandomSource rs = rsf.newRandomSource(dg);
        ExtenderService ext = new ExtenderService(dg, rs, config, output);
        dg.afterInsert(u -> ext.notify());
        dg.afterInsert(u -> {
            // don't put our own units on the unit belt, creator already knows about them.
            if (u.creator() != config.pid()) {
                unitBelt.add(u);
            }
        });
        return new epoch(id, adr, dg, ext, rs, null);
    }

    public void close() {
        adder.close();
        extender.close();
    }

    public List<Unit> unitsAbove(List<Integer> heights) {
        return dag.unitsAbove(heights);
    }

    public List<Unit> allUnits() {
        return dag.unitsAbove(null);
    }

    public boolean wantsMoreUnits() {
        Boolean m = more.poll();
        return m == null ? false : m;
    }

    public void noMoreUnits() {
    }
}
