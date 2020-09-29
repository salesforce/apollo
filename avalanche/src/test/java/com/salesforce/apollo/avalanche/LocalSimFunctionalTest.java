/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.comm.LocalCommSimm;

/**
 * @author hhildebrand
 *
 */
public class LocalSimFunctionalTest extends AvalancheFunctionalTest {

    protected Communications getCommunications() {
        return new LocalCommSimm();
    }
}
