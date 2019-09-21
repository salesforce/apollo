/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo;

import java.net.SocketException;
import java.net.URL;
import java.security.KeyStoreException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.ghost.Ghost;
import com.salesforce.apollo.ghost.MemoryStore;
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
        Apollo apollo = new Apollo(configuraton);
        apollo.start();
    }

    private final Avalanche avalanche;
    private final ApolloConfiguration configuration;
    private final Ghost ghost;
    private final AtomicBoolean running = new AtomicBoolean();
    private final View view;

    public Apollo(ApolloConfiguration c) throws SocketException, KeyStoreException {
        configuration = c;
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(configuration.threadPool);
        view = c.source.getIdentitySource(ApolloConfiguration.DEFAULT_CA_ALIAS,
                                          ApolloConfiguration.DEFAULT_IDENTITY_ALIAS)
                       .createView(configuration.communications.fireflies(), scheduler);
        ghost = new Ghost(configuration.ghost, configuration.communications.ghost(), view, new MemoryStore());
        avalanche = new Avalanche(view, configuration.communications.avalanche(), c.avalanche);
    }

    public Avalanche getAvalanche() {
        return avalanche;
    }

    public Ghost getGhost() {
        return ghost;
    }

    public View getView() {
        return view;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) { return; }
        view.getService().start(configuration.gossipInterval);
        ghost.getService().start(configuration.gossipInterval);
        avalanche.start();
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) { return; }
        view.getService().stop();
        ghost.getService().stop();
        avalanche.stop();
    }
}
