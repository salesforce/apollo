/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common;

import com.salesforce.apollo.snow.Context;

/**
 * @author hal.hildebrand
 *
 */
public interface Engine extends Handler { 
    Context getContext();

    boolean isBootstrapped();

    Object health();
} 
