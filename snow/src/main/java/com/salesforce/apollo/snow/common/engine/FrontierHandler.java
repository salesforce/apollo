/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.common.engine;

import java.util.Set;

import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
public interface FrontierHandler {

    void getAcceptedFrontier(ShortID validatorID, int requestID);

    void acceptedFrontier(ShortID validatorID, int requestId, Set<ID> containerIDs);

    void getAcceptedFrontierFailed(ShortID validatorID, int requestID);
}
