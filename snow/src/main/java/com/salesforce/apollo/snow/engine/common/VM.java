/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common;

/**
 * @author hal.hildebrand
 *
 */
public interface VM {
    /**
     * Called when the node is starting to bootstrap this chain
     */
    void bootstrapping();

    // Called when the node is shutting down
    void shutdown();
}
