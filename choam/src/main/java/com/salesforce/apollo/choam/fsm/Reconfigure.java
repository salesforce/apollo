/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.salesforce.apollo.choam.fsm.Driven.Transitions;

/**
 * @author hal.hildebrand
 *
 */
public enum Reconfigure implements Transitions {
    GATHER, NOMINATE, RECONFIGURED, CHANGE_PRINCIPAL;
}
