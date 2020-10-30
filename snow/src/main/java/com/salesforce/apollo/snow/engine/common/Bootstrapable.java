/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common;

import java.util.Set;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public interface Bootstrapable {
    // Returns the set of containerIDs that are accepted, but have no accepted
    // children.
    Set<ID> currentAcceptedFrontier();

    // Returns the subset of containerIDs that are accepted by this chain.
    Set<ID> filterAccepted(Set<ID> containerIDs);

    // Force the provided containers to be accepted. Only returns fatal errors
    // if they occur.
    void forceAccepted(Set<ID> accepted);

}
