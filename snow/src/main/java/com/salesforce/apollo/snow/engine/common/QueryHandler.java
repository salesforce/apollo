/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common;

import java.util.Set;

import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
public interface QueryHandler {
    void pullQuery(ShortID validatorID, int requestID, ID containerID);

    void pushQuery(ShortID validatorID, int requestID, ID containerID, byte[] container);

    void chits(ShortID validatorID, int requestID, Set<ID> containerIDs);

    void queryFailed(ShortID validatorID, int requestId);

}
