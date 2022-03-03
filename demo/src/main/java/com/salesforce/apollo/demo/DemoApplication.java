/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo;

import java.time.Duration;

import com.salesforce.apollo.model.Node;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * @author hal.hildebrand
 *
 */
public class DemoApplication extends Application<DemoConfiguration> {

    @SuppressWarnings("unused")
    private Node node;

    @Override
    public void run(DemoConfiguration configuration, Environment environment) throws Exception {
        environment.jersey().register(new KERLResource(null, Duration.ofSeconds(2)));
        environment.jersey().register(new DelphiResource(null, Duration.ofSeconds(2)));
        environment.healthChecks().register("demo", new DemoHealthCheck());
    }
}
