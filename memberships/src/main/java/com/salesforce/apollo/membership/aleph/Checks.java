/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * @author hal.hildebrand
 *
 */
public interface Checks {

    static final int MaxDtaBytesPerUnit              = (int) 2e6;
    static final int MaxRandomSourceDataBytesPerUnit = (int) 1e6;
    static final int MaxUnitsInChunk                 = (int) 1e6;

    static final List<BiConsumer<Unit, Dag>> SetupChecks     = Arrays.asList(basicCorrectness(), parentConsistency(),
                                                                             noLevelSkipping(), noForks());
    static final List<BiConsumer<Unit, Dag>> ConsensusChecks = Arrays.asList(basicCorrectness(), parentConsistency(),
                                                                             noSelfForkingEvidence(), forkerMuting());

    static BiConsumer<Unit, Dag> basicCorrectness() {
        return (u, dag) -> {
        };
    }

    static BiConsumer<Unit, Dag> parentConsistency() {
        return (u, dag) -> {
        };
    }

    static BiConsumer<Unit, Dag> noLevelSkipping() {
        return (u, dag) -> {
        };
    }

    static BiConsumer<Unit, Dag> noForks() {
        return (u, dag) -> {
        };
    }

    static BiConsumer<Unit, Dag> noSelfForkingEvidence() {
        return (u, dag) -> {
        };
    }

    static BiConsumer<Unit, Dag> forkerMuting() {
        return (u, dag) -> {
        };
    }
}
