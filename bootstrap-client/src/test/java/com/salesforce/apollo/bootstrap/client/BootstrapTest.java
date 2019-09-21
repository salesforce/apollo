/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.bootstrap.client;

import static org.junit.Assert.assertNotNull;

import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;

import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.junit.ClassRule;
import org.junit.Test;

import com.salesforce.apollo.bootstrap.BootstrapCA;
import com.salesforce.apollo.bootstrap.BootstrapConfiguration;
import com.salesforce.apollo.bootstrap.MintApi;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import io.github.olivierlemasle.ca.KeysUtil;

/**
 * @author hhildebrand
 */
public class BootstrapTest {

    @ClassRule
    public static final DropwizardAppRule<BootstrapConfiguration> RULE = new DropwizardAppRule<BootstrapConfiguration>(BootstrapCA.class,
                                                                                                                       ResourceHelpers.resourceFilePath("server.yml")) {

        @Override
        protected JerseyClientBuilder clientBuilder() {
            return super.clientBuilder().property(ClientProperties.CONNECT_TIMEOUT, 1000)
                                        .property(ClientProperties.READ_TIMEOUT, 60_000);
        }
    };

    @Test
    public void smoke() {
        Client client = RULE.client();
        final KeyPair pair = KeysUtil.generateKeyPair();
        WebTarget targetEndpoint = client.target(
                                                 String.format("http://localhost:%d/api/cnc/mint",
                                                               RULE.getLocalPort()));
        Bootstrap bootstrap = new Bootstrap(targetEndpoint, pair.getPublic(),
                                            forSigning(pair.getPrivate(), new SecureRandom()), "localhost", 0, 1, 2, 100, 30);
        assertNotNull(bootstrap);
        assertNotNull(bootstrap.getCa());
        assertNotNull(bootstrap.getIdentity());
        assertNotNull(bootstrap.getSeeds());
    }

    private Signature forSigning(PrivateKey privateKey, SecureRandom entropy) {
        Signature signature;
        try {
            signature = Signature.getInstance(MintApi.SIGNATURE_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + MintApi.SIGNATURE_ALGORITHM, e);
        }
        try {
            signature.initSign(privateKey, entropy);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid private key", e);
        }
        return signature;
    }
}
