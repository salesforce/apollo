/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import static com.salesforce.apollo.comm.grpc.MtlsServer.getMemberId;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.proto.Digests;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.Signed;
import com.salesfoce.apollo.proto.Update;
import com.salesforce.apollo.comm.grpc.ClientIdentity;
import com.salesforce.apollo.fireflies.CertWithKey;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

/**
 * A communications factory for local,non network communications for simulations
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class FfLocalCommSim implements FirefliesCommunications, ClientIdentity {
    private static class LocalServer {
        private final View   view;
        private final Server server;

        private LocalServer(View view, Server server) {
            this.view = view;
            this.server = server;
        }
    }

    private static final Logger                log             = LoggerFactory.getLogger(FfLocalCommSim.class);
    private final ThreadLocal<X509Certificate> callCertificate = new ThreadLocal<>();

    private volatile boolean                checkStarted;
    private final Map<HashKey, LocalServer> servers = new ConcurrentHashMap<>();
    private final String                    suffix  = UUID.randomUUID().toString();

    public FfLocalCommSim() {
    }

    public void checkStarted(boolean b) {
        this.checkStarted = b;
    }

    public void clear() {
        servers.values().forEach(ls -> {
            ls.server.shutdownNow();
        });
        servers.clear();
    }

    @Override
    public void close() {
    }

    @Override
    public FfClientCommunications connectTo(Participant to, Node from) {
        assert !to.getId().equals(from.getId());
        LocalServer localServer = servers.get(to.getId());
        if (localServer == null || (checkStarted && !localServer.view.getService().isStarted())) {
            log.debug("Unable to connect to: " + to + " from: " + from
                    + (localServer == null ? null : localServer.view.getService().isStarted()));
            return null;
        }
        ManagedChannel channel = InProcessChannelBuilder.forName(to.getId().b64Encoded() + suffix)
                                                        .directExecutor()
                                                        .build();
        return new FfClientCommunications(channel, to) {

            @Override
            public Gossip gossip(Signed note, int ring, Digests digests) {
                X509Certificate prev = callCertificate.get();
                callCertificate.set(from.getCertificate());
                try {
                    return super.gossip(note, ring, digests);
                } finally {
                    callCertificate.set(prev);
                }
            }

            @Override
            public int ping(int ping) {
                X509Certificate prev = callCertificate.get();
                callCertificate.set(from.getCertificate());
                try {
                    return super.ping(ping);
                } finally {
                    callCertificate.set(prev);
                }
            }

            @Override
            public void update(int ring, Update update) {
                X509Certificate prev = callCertificate.get();
                callCertificate.set(from.getCertificate());
                try {
                    super.update(ring, update);
                } finally {
                    callCertificate.set(prev);
                }
            }

        };
    }

    @Override
    public X509Certificate getCert() {
        return callCertificate.get();
    }

    @Override
    public Certificate[] getCerts() {
        return new Certificate[] { (Certificate) getCert() };
    }

    @Override
    public HashKey getFrom() {
        return getMemberId(getCert());
    }

    @Override
    public void initialize(View view) {
        log.debug("adding view: " + view.getNode().getId());
        try {
            Server server = InProcessServerBuilder.forName(view.getNode().getId().b64Encoded() + suffix)
                                                  .directExecutor() // directExecutor is fine for unit tests
                                                  .addService(new FfServerCommunications(view.getService(), this))
                                                  .build()
                                                  .start();
            servers.put(view.getNode().getId(), new LocalServer(view, server));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
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
    public void start() {
    }

}
