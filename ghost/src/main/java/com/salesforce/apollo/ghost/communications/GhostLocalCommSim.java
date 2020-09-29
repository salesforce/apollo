/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import static com.salesforce.apollo.comm.grpc.MtlsServer.getMemberId;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.Interval;
import com.salesforce.apollo.comm.grpc.ClientIdentity;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.ghost.Ghost;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class GhostLocalCommSim implements GhostCommunications, ClientIdentity {

    private static final Logger                log             = LoggerFactory.getLogger(GhostLocalCommSim.class);
    private final ThreadLocal<X509Certificate> callCertificate = new ThreadLocal<>();

    private final Map<HashKey, Server> servers = new ConcurrentHashMap<>();
    private final String               suffix  = UUID.randomUUID().toString();

    public GhostLocalCommSim() {
    }

    public void clear() {
        servers.values().forEach(s -> {
            s.shutdownNow();
        });
        servers.clear();
    }

    @Override
    public void close() {
    }

    public GhostClientCommunications connect(Member to, Node from) {
        ManagedChannel channel = InProcessChannelBuilder.forName(to.getId().b64Encoded() + suffix)
                                                        .directExecutor()
                                                        .build();
        return new GhostClientCommunications(channel, to) {

            @Override
            public DagEntry get(HashKey entry) {
                X509Certificate prev = callCertificate.get();
                callCertificate.set(from.getCertificate());
                try {
                    return super.get(entry);
                } finally {
                    callCertificate.set(prev);
                }
            }

            @Override
            public List<DagEntry> intervals(List<Interval> intervals, List<HashKey> have) {
                X509Certificate prev = callCertificate.get();
                callCertificate.set(from.getCertificate());
                try {
                    return super.intervals(intervals, have);
                } finally {
                    callCertificate.set(prev);
                }
            }

            @Override
            public void put(DagEntry value) {
                X509Certificate prev = callCertificate.get();
                callCertificate.set(from.getCertificate());
                try {
                    super.put(value);
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
    public void start() {
    }

    @Override
    public void initialize(Ghost ghost) {
        log.debug("adding view: " + ghost.getNode().getId());
        try {
            Server server = InProcessServerBuilder.forName(ghost.getNode().getId().b64Encoded() + suffix)
                                                  .directExecutor() // directExecutor is fine for unit tests
                                                  .addService(new GhostServerCommunications(ghost.getService(), this))
                                                  .build()
                                                  .start();
            servers.put(ghost.getNode().getId(), server);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
