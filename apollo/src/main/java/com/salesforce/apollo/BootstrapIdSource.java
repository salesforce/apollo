/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.InvalidKeyException;
import java.security.InvalidParameterException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import com.salesforce.apollo.ApolloConfiguration.IdentityStoreSource;
import com.salesforce.apollo.IdentitySource.BootstrapIdentitySource;
import com.salesforce.apollo.bootstrap.client.Bootstrap;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hhildebrand
 */
public class BootstrapIdSource implements IdentityStoreSource {
    public static final String ALGORITHM           = "RSA";
    public static final int    DEFAULT_KEY_SIZE    = 2048;
    public static final String SIGNATURE_ALGORITHM = "SHA256withRSA";

    public int    grpcPort              = 0;
    public String hostName;
    public String keyGeneratorAlgorithm = ALGORITHM;
    public int    keySize               = DEFAULT_KEY_SIZE;
    public int    retries               = 30;
    public long   retryPeriod           = 1_000;              // MS
    public String signatureAlgorithm    = SIGNATURE_ALGORITHM;
    public URL    endpoint;

    public KeyPair generateKeyPair() {
        try {
            final KeyPairGenerator gen = KeyPairGenerator.getInstance(keyGeneratorAlgorithm);
            gen.initialize(keySize);
            return gen.generateKeyPair();
        } catch (final NoSuchAlgorithmException | InvalidParameterException e) {
            throw new IllegalStateException("Unable to generate keypair", e);
        }
    }

    protected URI getEndpoint() throws URISyntaxException {
        return endpoint.toURI();
    }

    @Override
    public IdentitySource getIdentitySource(String caAlias, String identityAlias) {
        final KeyPair pair = generateKeyPair();
        Client client = ClientBuilder.newClient();
        URI uri;
        try {
            uri = getEndpoint();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Cannot convert endpoint to URI: " + endpoint.toExternalForm());
        }
        WebTarget targetEndpoint = client.target(uri);

        Bootstrap bootstrap = new Bootstrap(targetEndpoint, pair.getPublic(),
                forSigning(pair.getPrivate(), new SecureRandom()), getHostName(), getGrpcPort(), retryPeriod, retries);
        return new BootstrapIdentitySource(bootstrap, pair.getPrivate());
    }

    protected Signature forSigning(PrivateKey privateKey, SecureRandom entropy) {
        Signature signature;
        try {
            signature = Signature.getInstance(signatureAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("no such algorithm: " + signatureAlgorithm, e);
        }
        try {
            signature.initSign(privateKey, entropy);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException("invalid private key", e);
        }
        return signature;
    }

    protected int getGrpcPort() {
        return grpcPort != 0 ? grpcPort : Utils.allocatePort();
    }

    protected String getHostName() {
        if (hostName != null) {
            return hostName;
        } else {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new IllegalStateException("Unable to determine local host name", e);
            }
        }
    }
}
