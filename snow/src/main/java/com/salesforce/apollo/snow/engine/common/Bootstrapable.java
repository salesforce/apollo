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

    void forceAccepted(Set<ID> accepted);

    Set<ID> filterAccepted(Set<ID> containerIDs);

    Set<ID> currentAcceptedFrontier();

}
