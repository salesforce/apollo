/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowstorm;

import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.consensus.snowball.Parameters;

/**
 * @author hal.hildebrand
 *
 */
public class DirectedTest extends ConsensusTest {

    @Override
    public Consensus createConsensus(Context ctx, Parameters params) {
        return new Directed(ctx, params);
    }
    
}
