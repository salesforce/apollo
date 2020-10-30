/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public interface EventDispatcher {
    public void issue(Context ctx, ID containerID, byte[] container);
    public void accept(Context ctx, ID containerID, byte[] container);
    public void reject(Context ctx, ID containerID, byte[] container);
}
