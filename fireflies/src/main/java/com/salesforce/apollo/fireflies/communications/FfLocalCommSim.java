/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies.communications;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.ipc.LocalTransceiver;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.avro.Apollo;
import com.salesforce.apollo.fireflies.CertWithKey;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;

/**
 * A communications factory for local,non network communications for simulations
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class FfLocalCommSim implements FirefliesCommunications {
    private static final Logger log = LoggerFactory.getLogger(FfLocalCommSim.class);

    private volatile boolean checkStarted;
    private final Map<UUID, View> servers = new ConcurrentHashMap<>();
    private final RPCPlugin stats;

    public FfLocalCommSim() {
        this(null);
    }

    public FfLocalCommSim(RPCPlugin stats) {
        this.stats = stats;
    }

    public void checkStarted(boolean b) {
        this.checkStarted = b;
    }

    @Override
    public void close() {}

    @Override
    public FfClientCommunications connectTo(Member to, Node from) {
        View view = servers.get(to.getId());
        if (view == null || (checkStarted && !view.getService().isStarted())) {
            log.debug("Unable to connect to: " + to + " from: " + from
                    + (view == null ? null : view.getService().isStarted()));
            return null;
        }
        SpecificResponder responder = new SpecificResponder(Apollo.PROTOCOL,
                                                            new FfServerCommunications(view.getService(),
                                                                                       from.getCertificate()));

        FfClientCommunications clientCommunications = new FfClientCommunications(new LocalTransceiver(responder), to);
        if (stats != null) {
            responder.addRPCPlugin(stats);
            clientCommunications.add(stats);
        }
        return clientCommunications;
    }

    public Map<UUID, View> getServers() {
        return servers;
    }

    public RPCPlugin getStats() {
        return stats;
    }

    @Override
    public void initialize(View view) {
        servers.put(view.getNode().getId(), view);
    }

    @Override
    public Node newNode(CertWithKey identity, FirefliesParameters parameters) {
        return new Node(identity, parameters);
    }

    @Override
    public Node newNode(CertWithKey identity, FirefliesParameters parameters, InetSocketAddress[] boundPorts) {
        return new Node(identity, parameters, boundPorts);
    }

    @Override
    public void start() {}

}
