/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.web;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.salesforce.apollo.Apollo;
import com.salesforce.apollo.avalanche.Processor.TimedProcessor;
import com.salesforce.apollo.web.resources.ApolloHealthCheck;
import com.salesforce.apollo.web.resources.AvalancheHealthCheck;
import com.salesforce.apollo.web.resources.ByteTransactionApi;
import com.salesforce.apollo.web.resources.DagApi;
import com.salesforce.apollo.web.resources.FirefliesHealthCheck;
import com.salesforce.apollo.web.resources.GenesisBlockApi;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

/**
 * @author hhildebrand
 */
public class ApolloService extends Application<ApolloServiceConfiguration> {

    public static void main(String[] argv) throws Exception {
        ApolloService apolloService = new ApolloService();
        apolloService.run(argv);
    }

    private volatile Apollo apollo;

    public Apollo getApollo() {
        return apollo;
    }

    @Override
    public void run(ApolloServiceConfiguration configuration, Environment environment) throws Exception {
        apollo = new Apollo(configuration.getApollo(), environment.metrics());
        apollo.start();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10, r -> {
            Thread daemon = new Thread(r, "Apollo Txn Timeout Scheduler");
            return daemon;
        });
        TimedProcessor processor = apollo.getProcessor();

        environment.healthChecks().register("fireflies", new FirefliesHealthCheck(apollo.getView()));
        environment.healthChecks().register("avalanche", new AvalancheHealthCheck(processor));
        environment.healthChecks().register("Apollo", new ApolloHealthCheck(apollo));

        environment.jersey().register(new ByteTransactionApi(processor, scheduler));
        environment.jersey().register(new GenesisBlockApi(processor, scheduler));
        environment.jersey().register(new DagApi(apollo.getAvalanche().getDagDao()));
    }

    public void start() {
        apollo.start();
    }

    public void stop() {
        apollo.stop();
    }
}
