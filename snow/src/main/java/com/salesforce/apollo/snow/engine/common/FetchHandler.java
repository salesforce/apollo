/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common;

import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
public interface FetchHandler {
    void get(ShortID validatorId, int requestId, ID containerID);

    void getAncestors(ShortID validatorId, int requestID, ID containerID);

    void put(ShortID validatorID, int requestID, ID containerID, byte[] container);

    void multiPut(ShortID validatorID, int requestId, byte[][] containers);

    void getFailed(ShortID validatorID, int requestID);

    void getAncestorsFailed(ShortID validatorID, int requestID);
}
