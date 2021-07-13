/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.aleph;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface Adder {

    enum Correctness {
        CORRECT, DATA_ERROR, DUPLICATE_UNIT, DUPLICATE_PRE_UNIT, UNKNOWN_PARENTS;
    }

    record waitingPreUnit(PreUnit pu, long id, long source, AtomicInteger missingParents, AtomicInteger waitingParents,
                          List<waitingPreUnit> children, AtomicBoolean failed) {}

    record missingPreUnit(List<waitingPreUnit> neededBy, java.time.Instant requested) {}

    // Checks basic correctness of a slice of preunits and then adds
    // correct ones to the buffer zone.
    // Returned slice can have the following members:
    // - DataError - if creator or signature are wrong
    // - DuplicateUnit, DuplicatePreunit - if such a unit is already in dag/waiting
    // - UnknownParents - in that case the preunit is normally added and processed,
    // error is returned only for log purpose.
    Map<Digest, Correctness> addPreunits(short source, List<PreUnit> preunits);

}
