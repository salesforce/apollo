/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo;

import java.io.File;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.time.Duration;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.salesforce.apollo.IdentitySource.DefaultIdentitySource;
import com.salesforce.apollo.avalanche.AvalancheParameters;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.comm.EndpointProvider;
import com.salesforce.apollo.comm.LocalCommSimm;
import com.salesforce.apollo.comm.MtlsCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache;
import com.salesforce.apollo.fireflies.FireflyMetricsImpl;
import com.salesforce.apollo.ghost.Ghost.GhostParameters;

/**
 * @author hal.hildebrand
 * @since 218
 */
public class ApolloConfiguration {
    public interface CommunicationsFactory {

        Communications getComms(MetricRegistry metrics);

    }

    public static class FileIdentitySource implements IdentityStoreSource {

        public char[] password = DEFAULT_PASSWORD;
        public File   store    = new File(".keystore");
        public String type     = DEFAULT_TYPE;

        @Override
        public IdentitySource getIdentitySource(String caAlias, String identityAlias) {
            try {
                return new DefaultIdentitySource(caAlias, store, type, identityAlias, password);
            } catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException e) {
                throw new IllegalStateException("Cannot create file identity source", e);
            }
        }

    }

    public interface IdentityStoreSource {
        IdentitySource getIdentitySource(String caAlias, String identityAlias);
    }

    public static class ResourceIdentitySource implements IdentityStoreSource {

        public char[] password = DEFAULT_PASSWORD;
        public String store    = ".keystore";
        public String type     = DEFAULT_TYPE;

        @Override
        public IdentitySource getIdentitySource(String caAlias, String identityAlias) {
            try {
                return new DefaultIdentitySource(caAlias, store, type, identityAlias, password);
            } catch (UnrecoverableKeyException | KeyStoreException | NoSuchAlgorithmException e) {
                throw new IllegalStateException("Cannot create resource identity source", e);
            }
        }
    }

    public static class MtlsCommunicationsFactory implements CommunicationsFactory {
        public int target = 30;

        @Override
        public Communications getComms(MetricRegistry metrics) {
            EndpointProvider ep = null;
            return new MtlsCommunications(
                    ServerConnectionCache.newBuilder().setTarget(target).setMetrics(new FireflyMetricsImpl(metrics)),
                    ep);
        }

    }

    public static class SimCommunicationsFactory implements CommunicationsFactory {

        public static LocalCommSimm LOCAL_COM;

        static {
            reset();
        }

        public static void reset() {
            if (LOCAL_COM != null) {
                LOCAL_COM.close();
                LOCAL_COM = null;
            }
        }

        public int target = 30;

        @Override
        public Communications getComms(MetricRegistry metrics) {
            if (LOCAL_COM == null) {
                LOCAL_COM = new LocalCommSimm(ServerConnectionCache.newBuilder()
                                                                   .setTarget(target)
                                                                   .setMetrics(new FireflyMetricsImpl(metrics)));
            }
            return LOCAL_COM;
        }

    }

    public static final String   DEFAULT_CA_ALIAS        = "CA";
    public static final Duration DEFAULT_GOSSIP_INTERVAL = Duration.ofMillis(500);
    public static final String   DEFAULT_IDENTITY_ALIAS  = "identity";
    public static final char[]   DEFAULT_PASSWORD        = "".toCharArray();
    public static final String   DEFAULT_TYPE            = "PKCS12";

    public AvalancheParameters   avalanche      = new AvalancheParameters();
    public long                  bufferSize     = 100 * 1024;
    public String                ca             = DEFAULT_CA_ALIAS;
    @JsonSubTypes({ @Type(value = SimCommunicationsFactory.class, name = "sim"),
                    @Type(value = MtlsCommunications.class, name = "mtls") })
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    public CommunicationsFactory communications = new MtlsCommunicationsFactory();
    public GhostParameters       ghost          = new GhostParameters();
    public Duration              gossipInterval = DEFAULT_GOSSIP_INTERVAL;
    public String                identity       = DEFAULT_IDENTITY_ALIAS;
    @JsonSubTypes({ @Type(value = FileIdentitySource.class, name = "file"),
                    @Type(value = ResourceIdentitySource.class, name = "resource"),
                    @Type(value = BootstrapIdSource.class, name = "bootstrap"),
                    @Type(value = EnvironmentConfiguredIdSource.class, name = "env") })
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    public IdentityStoreSource   source;
    public int                   threadPool     = 1;
}
