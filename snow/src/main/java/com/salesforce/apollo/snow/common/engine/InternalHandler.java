/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.common.engine;

import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
public interface InternalHandler {
    void startUp();

    void gossip();

    void shutdown();

    void Notify();

    void connectected(ShortID validatorId);

    void disconnected(ShortID validatorId);

}
