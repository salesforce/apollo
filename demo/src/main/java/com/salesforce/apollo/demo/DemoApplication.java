/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.demo;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * @author hal.hildebrand
 *
 */
public class DemoApplication extends Application<DemoConfiguration> {

    @Override
    public void run(DemoConfiguration configuration, Environment environment) throws Exception {
        environment.jersey().register(new ResolverResource(null));
        environment.jersey().register(new DelphiResource(null));
        environment.healthChecks().register("demo", new DemoHealthCheck());
    }
}
