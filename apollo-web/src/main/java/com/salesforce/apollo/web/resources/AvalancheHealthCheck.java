/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.web.resources;

import com.codahale.metrics.health.HealthCheck;
import com.salesforce.apollo.avalanche.Processor.TimedProcessor;

/**
 * @author hhildebrand
 */
public class AvalancheHealthCheck extends HealthCheck {

    @SuppressWarnings("unused")
    private final TimedProcessor processor;

    public AvalancheHealthCheck(TimedProcessor procesor) {
        this.processor = procesor;
    }

    @Override
    protected Result check() throws Exception {
        return Result.healthy();
    }

}
