/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
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
import com.salesforce.apollo.protocols.Utils;

/**
 * @author hhildebrand
 */
public class BootstrapIdSource implements IdentityStoreSource {
    public static final String ALGORITHM = "RSA";
    public static final int DEFAULT_KEY_SIZE = 2048;
    public static final String SIGNATURE_ALGORITHM = "SHA256withRSA";

    public int avalanchePort = 0;
    public URL endpoint;
    public int firefliesPort = 0;
    public int ghostPort = 0;
    public String hostName;
    public String keyGeneratorAlgorithm = ALGORITHM;
    public int keySize = DEFAULT_KEY_SIZE;
    public int retries = 30;
    public long retryPeriod = 1_000; // MS
    public String signatureAlgorithm = SIGNATURE_ALGORITHM;

    public KeyPair generateKeyPair() {
        try {
            final KeyPairGenerator gen = KeyPairGenerator.getInstance(keyGeneratorAlgorithm);
            gen.initialize(keySize);
            return gen.generateKeyPair();
        } catch (final NoSuchAlgorithmException | InvalidParameterException e) {
            throw new IllegalStateException("Unable to generate keypair", e);
        }
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
                                            forSigning(pair.getPrivate(), new SecureRandom()), getHostName(),
                                            getFirefliesPort(),
                                            getGhostPort(), getAvalanchePort(), retryPeriod, retries);
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

    protected int getAvalanchePort() {
        return avalanchePort != 0 ? avalanchePort : Utils.allocatePort();
    }

    protected URI getEndpoint() throws URISyntaxException {
        return endpoint.toURI();
    }

    protected int getFirefliesPort() {
        return firefliesPort != 0 ? firefliesPort : Utils.allocatePort();
    }

    protected int getGhostPort() {
        return ghostPort != 0 ? ghostPort : Utils.allocatePort();
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
