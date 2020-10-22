/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.snowman;

import com.salesforce.apollo.snow.consensus.snowman.Consensus;

/**
 * @author hal.hildebrand
 *
 */
public class Config {
    public final Parameters params;
    public final Consensus  consensus;

    public Config(Parameters params, Consensus consensus) {
        this.params = params;
        this.consensus = consensus;
    }
}
