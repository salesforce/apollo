/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import com.salesforce.apollo.bootstrap.client.Bootstrap;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View; 
import com.salesforce.apollo.membership.CertWithKey;
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hhildebrand
 */
public interface IdentitySource {

    class BootstrapIdentitySource implements IdentitySource {
        private final Bootstrap  bootstrap;
        private final PrivateKey privateKey;

        public BootstrapIdentitySource(Bootstrap bootstrap, PrivateKey privateKey) {
            this.bootstrap = bootstrap;
            this.privateKey = privateKey;
        }

        @Override
        public X509Certificate getCA() {
            return bootstrap.getCa();
        }

        @Override
        public CertWithKey identity() {
            return new CertWithKey(bootstrap.getIdentity(), privateKey);
        }

        @Override
        public List<X509Certificate> seeds() {
            return bootstrap.getSeeds();
        }

    }

    class DefaultIdentitySource implements IdentitySource {
        static KeyStore getKeystore(String type, File store, char[] password) throws KeyStoreException {
            try (InputStream is = new FileInputStream(store)) {
                return getKeystore(type, is, password);
            } catch (FileNotFoundException e) {
                throw new IllegalStateException("Unable to find store: " + store.getAbsolutePath(), e);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to read store: " + store.getAbsolutePath(), e);
            }
        }

        static KeyStore getKeystore(String type, InputStream is, char[] password) throws KeyStoreException {
            KeyStore keystore = KeyStore.getInstance(type);
            try {
                keystore.load(is, password);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to read store ", e);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalArgumentException("invalid type: " + type, e);
            } catch (CertificateException e) {
                throw new IllegalArgumentException("invalid certificate", e);
            }
            return keystore;
        }

        static KeyStore getKeystore(String type, String store, char[] password) throws KeyStoreException {

            URL url;
            try {
                url = Utils.resolveResourceURL(ApolloConfiguration.class, store);
            } catch (IOException e) {
                throw new IllegalArgumentException("KeyStore not found: " + store, e);
            }
            if (url == null) {
                throw new IllegalArgumentException("KeyStore not found: " + store);
            }
            try (InputStream is = url.openStream()) {
                return getKeystore(type, is, password);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to read store resource: " + store, e);
            }
        }

        static List<X509Certificate> seedsFrom(KeyStore ks) {
            List<X509Certificate> seeds = new ArrayList<>();
            Enumeration<String> aliases;
            try {
                aliases = ks.aliases();
            } catch (KeyStoreException e) {
                throw new IllegalStateException();
            }
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (alias.startsWith(SEED_PREFIX)) {
                    try {
                        seeds.add((X509Certificate) ks.getCertificate(alias));
                    } catch (KeyStoreException e) {
                        throw new IllegalStateException("Unable to get seed certificate for alias: " + alias, e);
                    }
                }
            }
            return seeds;
        }

        private final X509Certificate       ca;
        private final CertWithKey           identity;
        private final List<X509Certificate> seeds;

        public DefaultIdentitySource(CertWithKey identity, List<X509Certificate> seeds, X509Certificate ca) {
            this.identity = identity;
            this.seeds = seeds;
            this.ca = ca;
        }

        public DefaultIdentitySource(KeyStore keystore, char[] password)
                throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException {
            this(DEFAULT_CA_ALIAS, keystore, DEFAULT_IDENTITY_ALIAS, password);
        }

        public DefaultIdentitySource(String caAlias, File store, String type, String identityAlias, char[] password)
                throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException {
            this(caAlias, getKeystore(type, store, password), identityAlias, password);
        }

        public DefaultIdentitySource(String caAlias, KeyStore keystore, String identityAlias, char[] password)
                throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException {
            this(new CertWithKey((X509Certificate) keystore.getCertificate(identityAlias),
                    (PrivateKey) keystore.getKey(identityAlias, password)), seedsFrom(keystore),
                    (X509Certificate) keystore.getCertificate(caAlias));
        }

        public DefaultIdentitySource(String caAlias, String store, String type, String identityAlias, char[] password)
                throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException {
            this(caAlias, getKeystore(type, store, password), identityAlias, password);
        }

        @Override
        public X509Certificate getCA() {
            return ca;
        }

        @Override
        public CertWithKey identity() {
            return identity;
        }

        @Override
        public List<X509Certificate> seeds() {
            return seeds;
        }

    }

    class MappingIdentitySource extends BootstrapIdentitySource {
        private final int    avalanchePort;
        private final int    firefliesPort;
        private final int    ghostPort;
        private final String hostName;

        /**
         * @param bootstrap
         * @param privateKey
         */
        public MappingIdentitySource(Bootstrap bootstrap, PrivateKey privateKey, String hostName, int firefliesPort,
                int ghostPort, int avalanchePort) {
            super(bootstrap, privateKey);
            this.hostName = hostName;
            this.firefliesPort = firefliesPort;
            this.ghostPort = ghostPort;
            this.avalanchePort = avalanchePort;
        }

        @Override
        public View createView(Communications communications, ScheduledExecutorService scheduler) {
            FirefliesParameters parameters = new FirefliesParameters(getCA());

            InetSocketAddress[] boundPorts = new InetSocketAddress[] { new InetSocketAddress(hostName, firefliesPort),
                                                                       new InetSocketAddress(hostName, ghostPort),
                                                                       new InetSocketAddress(hostName, avalanchePort) };

            return new View(communications.newNode(identity(), parameters, boundPorts), communications, scheduler);
        }
    }

    public static final String DEFAULT_CA_ALIAS       = "CA";
    public static final String DEFAULT_IDENTITY_ALIAS = "identity";
    public static final String SEED_PREFIX            = "seed.";

    default <T extends Node> View createView(Communications communications,
                                             ScheduledExecutorService scheduler) {
        FirefliesParameters parameters = new FirefliesParameters(getCA());

        return new View(communications.newNode(identity(), parameters), communications, scheduler);
    }

    X509Certificate getCA();

    CertWithKey identity();

    List<X509Certificate> seeds();
}
