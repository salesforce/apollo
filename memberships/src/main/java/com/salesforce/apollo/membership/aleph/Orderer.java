/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import java.util.List;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.aleph.Dag.DagInfo;
import com.salesforce.apollo.membership.aleph.RandomSource.RandomSourceFactory;

/**
 * Orderer orders ordered orders into ordered order.
 * 
 * @author hal.hildebrand
 *
 */
public interface Orderer {
    // AddPreunits sends to orderer preunits received from other committee member.
    void addPreunits(short orderer, List<PreUnit> preunits);

    // UnitsByID finds units with given IDs in Orderer.
    // Returns nil on the corresponding position if the requested unit is not
    // present.
    // In case of forks returns all known units with a particular ID.
    List<Unit> unitsById(List<Long> units);

    // UnitsByHash finds units with given IDs in Orderer.
    // Returns nil on the corresponding position if the requested unit is not
    // present.
    List<Unit> unitsByHash(List<Digest> digests);

    // MaxUnits returns maximal units per process for the given epoch. Returns nil
    // if epoch not known.
    SlottedUnits maxUnits(int epoch);

    // GetInfo returns DagInfo of the newest epoch.
    DagInfo[] getInfo();

    // Delta returns all the units present in orderer that are above heights
    // indicated by provided DagInfo. That includes also all units from newer
    // epochs.
    List<Unit> delta(DagInfo[] heights);

    // Start starts the orderer using provided RandomSourceFactory, Syncer, and
    // Alerter.
    void start(RandomSourceFactory factory, Syncer syncer, Alerter alerter);

    void stop();

}
