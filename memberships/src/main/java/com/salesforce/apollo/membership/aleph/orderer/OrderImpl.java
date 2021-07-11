/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph.orderer;

import java.util.List;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.aleph.Alerter;
import com.salesforce.apollo.membership.aleph.Dag.DagInfo;
import com.salesforce.apollo.membership.aleph.Orderer;
import com.salesforce.apollo.membership.aleph.PreUnit;
import com.salesforce.apollo.membership.aleph.RandomSource.RandomSourceFactory;
import com.salesforce.apollo.membership.aleph.SlottedUnits;
import com.salesforce.apollo.membership.aleph.Syncer;
import com.salesforce.apollo.membership.aleph.Unit;

/**
 * @author hal.hildebrand
 *
 */
public class OrderImpl implements Orderer {

    @Override
    public void addPreunits(short orderer, List<PreUnit> preunits) {
        // TODO Auto-generated method stub

    }

    @Override
    public List<Unit> unitsById(List<Long> units) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Unit> unitsByHash(List<Digest> digests) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SlottedUnits maxUnits(int epoch) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DagInfo[] getInfo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Unit> delta(DagInfo[] heights) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void start(RandomSourceFactory factory, Syncer syncer, Alerter alerter) {
        // TODO Auto-generated method stub

    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub

    }

}
