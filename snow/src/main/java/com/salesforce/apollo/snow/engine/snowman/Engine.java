/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.snowman;

/**
 * @author hal.hildebrand
 *
 */
public interface Engine extends com.salesforce.apollo.snow.engine.common.Engine{
    void initialize(Config configuration);
}
