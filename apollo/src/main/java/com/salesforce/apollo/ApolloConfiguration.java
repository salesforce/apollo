/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo;

import java.time.Duration;
import java.util.concurrent.ForkJoinPool;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.salesforce.apollo.avalanche.AvalancheParameters;
import com.salesforce.apollo.comm.EndpointProvider;
import com.salesforce.apollo.comm.LocalRouter;
import com.salesforce.apollo.comm.MtlsRouter;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.fireflies.FireflyMetricsImpl;
import com.salesforce.apollo.ghost.Ghost.GhostParameters;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 * @since 218
 */
public class ApolloConfiguration {
    public interface CommunicationsFactory {

        Router getComms(MetricRegistry metrics, SigningMember node, ForkJoinPool executor);

    }

    public interface IdentityStoreSource {
        IdentitySource getIdentitySource(String caAlias, String identityAlias);
    }

    public static class MtlsCommunicationsFactory implements CommunicationsFactory {
        public int target = 30;

        @Override
        public Router getComms(MetricRegistry metrics, SigningMember n, ForkJoinPool executor) {
            EndpointProvider ep = null;
            return new MtlsRouter(
                    ServerConnectionCache.newBuilder().setTarget(target).setMetrics(new FireflyMetricsImpl(metrics)),
                    ep, n, null, executor);
        }

    }

    public static class SimCommunicationsFactory implements CommunicationsFactory {

        public int target = 30;

        @Override
        public Router getComms(MetricRegistry metrics, SigningMember node, ForkJoinPool executor) {
            return new LocalRouter(node,
                    ServerConnectionCache.newBuilder().setTarget(target).setMetrics(new FireflyMetricsImpl(metrics)),
                    executor);
        }

    }

    public static final Duration DEFAULT_GOSSIP_INTERVAL = Duration.ofMillis(500);
    public static final char[]   DEFAULT_PASSWORD        = "".toCharArray();
    public static final Duration DEFAULT_QUERY_INTERVAL  = Duration.ofMillis(50);

    public AvalancheParameters   avalanche          = new AvalancheParameters();
    public long                  bufferSize         = 100 * 1024;
    @JsonSubTypes({ @Type(value = SimCommunicationsFactory.class, name = "sim"),
                    @Type(value = MtlsCommunicationsFactory.class, name = "mtls") })
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    public CommunicationsFactory communications     = new MtlsCommunicationsFactory();
    public Digest                contextBase        = DigestAlgorithm.DEFAULT.getOrigin().prefix(1);
    public DigestAlgorithm       digestAlgorithm    = DigestAlgorithm.DEFAULT;
    public GhostParameters       ghost              = new GhostParameters();
    public Duration              gossipInterval     = DEFAULT_GOSSIP_INTERVAL;
    public Duration              queryInterval      = DEFAULT_QUERY_INTERVAL;
    public SignatureAlgorithm    signatureAlgorithm = SignatureAlgorithm.DEFAULT;
    @JsonSubTypes({ @Type(value = BootstrapIdSource.class, name = "bootstrap"),
                    @Type(value = EnvironmentConfiguredIdSource.class, name = "env") })
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    public IdentityStoreSource   source;
    public int                   threadPool         = 1;
}
