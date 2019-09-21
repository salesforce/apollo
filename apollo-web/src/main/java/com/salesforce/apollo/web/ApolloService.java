/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.web;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.salesforce.apollo.Apollo;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.web.resources.ApolloHealthCheck;
import com.salesforce.apollo.web.resources.AvalancheHealthCheck;
import com.salesforce.apollo.web.resources.ByteTransactionApi;
import com.salesforce.apollo.web.resources.DagApi;
import com.salesforce.apollo.web.resources.FirefliesHealthCheck;
import com.salesforce.apollo.web.resources.GenesisBlockApi;
import com.salesforce.apollo.web.resources.GhostHealthCheck;

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
        apollo = new Apollo(configuration.getApollo());
        apollo.start();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10, r -> {
            Thread daemon = new Thread(r, "Apollo Txn Timeout Scheduler");
            return daemon;
        });
        Avalanche avalanche = apollo.getAvalanche();
        
        environment.healthChecks().register("fireflies", new FirefliesHealthCheck(apollo.getView()));
        environment.healthChecks().register("ghost", new GhostHealthCheck(apollo.getGhost()));
        environment.healthChecks().register("avalanche", new AvalancheHealthCheck(avalanche));
        environment.healthChecks().register("Apollo", new ApolloHealthCheck(apollo));
        
        environment.jersey()
                   .register(new ByteTransactionApi(avalanche, scheduler));
        environment.jersey()
                   .register(new GenesisBlockApi(avalanche, scheduler));
        environment.jersey()
                   .register(new DagApi(avalanche.getDagDao()));
    }

    public void start() {
        apollo.start();
    }

    public void stop() {
        apollo.stop();
    }
}
