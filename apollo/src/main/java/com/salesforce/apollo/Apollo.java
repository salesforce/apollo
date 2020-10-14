/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo;

import java.net.SocketException;
import java.net.URL;
import java.security.KeyStoreException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.salesforce.apollo.avalanche.AvaMetrics;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hal.hildebrand
 * @since 218
 */
public class Apollo {

    public static final String SEED_PREFIX = "seed.";

    public static void main(String[] argv) throws Exception {
        if (argv.length != 1) {
            System.err.println("usage: Apollo <configuration resource>");
            System.exit(1);
        }
        URL yaml = Utils.resolveResourceURL(Apollo.class, argv[0]);
        if (yaml == null) {
            System.err.println("cannot find configuration resource: " + argv[0]);
            System.exit(1);
        }
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        ApolloConfiguration configuraton = mapper.readValue(yaml.openStream(), ApolloConfiguration.class);
        Apollo apollo = new Apollo(configuraton, new MetricRegistry());
        apollo.start();
    }

    private final Avalanche             avalanche;
    private final ApolloConfiguration   configuration;
    private final AtomicBoolean         running = new AtomicBoolean();
    private final View                  view;
    private final Communications        communications;
    private final List<X509Certificate> seeds;

    public Apollo(ApolloConfiguration config) throws SocketException, KeyStoreException {
        this(config, new MetricRegistry());
    }

    public Apollo(ApolloConfiguration c, MetricRegistry metrics) throws SocketException, KeyStoreException {
        configuration = c;
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(configuration.threadPool);
        IdentitySource identitySource = c.source.getIdentitySource(ApolloConfiguration.DEFAULT_CA_ALIAS,
                                                                   ApolloConfiguration.DEFAULT_IDENTITY_ALIAS);
        HashKey id = Member.getMemberId(identitySource.identity().getCertificate());
        communications = c.communications.getComms(metrics, id);
        view = identitySource.createView(communications, scheduler);
        seeds = identitySource.seeds();
        avalanche = new Avalanche(view, communications, c.avalanche, metrics == null ? null : new AvaMetrics(metrics));
    }

    public Avalanche getAvalanche() {
        return avalanche;
    }

    public View getView() {
        return view;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        view.getService().start(configuration.gossipInterval, seeds);
        avalanche.start();
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        view.getService().stop();
        avalanche.stop();
    }
}
