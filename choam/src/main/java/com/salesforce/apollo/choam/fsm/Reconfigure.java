/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

/**
 * @author hal.hildebrand
 *
 */
public enum Reconfigure implements DrivenTransitions {
    GATHER, NOMINATE, RECONFIGURED;
}
