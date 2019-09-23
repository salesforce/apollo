/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.web.resources;

import com.codahale.metrics.health.HealthCheck;
import com.salesforce.apollo.ghost.Ghost;

/**
 * @author hhildebrand
 */
public class GhostHealthCheck extends HealthCheck {

    @SuppressWarnings("unused")
    private final Ghost ghost;

    public GhostHealthCheck(Ghost ghost) {
        this.ghost = ghost;
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }

}
