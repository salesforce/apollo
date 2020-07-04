/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import org.junit.jupiter.api.BeforeEach;

import com.salesforce.apollo.avalanche.communications.AvalancheCommunications;
import com.salesforce.apollo.avalanche.communications.AvalancheLocalCommSim;

/**
 * @author hhildebrand
 *
 */
public class LocalSimFunctionalTest extends AvalancheFunctionalTest {

    private AvalancheLocalCommSim localComSim;

    @BeforeEach
    public void beforeTest() {
        localComSim = new AvalancheLocalCommSim(rpcStats);
    }

    protected AvalancheCommunications getCommunications() {
        return localComSim;
    }
}
