/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

/**
 * @author hhildebrand
 *
 */
public class TlsFunctionalTest extends AvalancheFunctionalTest {

    protected AvalancheCommunications getCommunications() {
        return new AvalancheNettyCommunications(rpcStats, eventLoop, eventLoop, eventLoop, executor, executor);
    }

}
