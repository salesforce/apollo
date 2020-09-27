/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import static com.salesforce.apollo.comm.grpc.MtlsServer.getMemberId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.proto.QueryResult;
import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.comm.grpc.ClientIdentity;
import com.salesforce.apollo.fireflies.Node;
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
public class AvalancheLocalCommSim implements AvalancheCommunications, ClientIdentity {

    private static final Logger                log             = LoggerFactory.getLogger(AvalancheLocalCommSim.class);
    private final ThreadLocal<X509Certificate> callCertificate = new ThreadLocal<>();

    private final Map<HashKey, Server> servers = new ConcurrentHashMap<>();
    private final String               suffix  = UUID.randomUUID().toString();

    public AvalancheLocalCommSim() {
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

    @Override
    public AvalancheClientCommunications connectToNode(Member to, Node from) {
        ManagedChannel channel = InProcessChannelBuilder.forName(to.getId().b64Encoded() + suffix)
                                                        .directExecutor()
                                                        .build();
        return new AvalancheClientCommunications(channel, to) {

            @Override
            public QueryResult query(List<ByteBuffer> transactions, Collection<HashKey> wanted) {
                X509Certificate prev = callCertificate.get();
                callCertificate.set(from.getCertificate());
                try {
                    return super.query(transactions, wanted);
                } finally {
                    callCertificate.set(prev);
                }
            }

            @Override
            public List<ByteBuffer> requestDAG(Collection<HashKey> want) {
                X509Certificate prev = callCertificate.get();
                callCertificate.set(from.getCertificate());
                try {
                    return super.requestDAG(want);
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
    public void initialize(Avalanche avalanche) {
        log.debug("adding view: " + avalanche.getNode().getId());
        try {
            Server server = InProcessServerBuilder.forName(avalanche.getNode().getId().b64Encoded() + suffix)
                                                  .directExecutor() // directExecutor is fine for unit tests
                                                  .addService(new AvalancheServerCommunications(avalanche.getService(),
                                                          this))
                                                  .build()
                                                  .start();
            servers.put(avalanche.getNode().getId(), server);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
