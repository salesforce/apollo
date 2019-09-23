/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.bootstrap.client;

import java.io.ByteArrayInputStream;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hhildebrand
 */
public class Bootstrap {
    public static class MintRequest {
        private int avalanchePort;
        private int firefliesPort;
        private int ghostPort;
        private String hostname;
        private String publicKey;
        private String signature;

        public MintRequest() {}

        public MintRequest(String hostname, int firefliesPort, int ghostPort, int avalanchePort, String publicKey,
                String signature) {
            this.hostname = hostname;
            this.firefliesPort = firefliesPort;
            this.ghostPort = ghostPort;
            this.avalanchePort = avalanchePort;
            this.publicKey = publicKey;
            this.signature = signature;
        }

        public int getAvalanchePort() {
            return avalanchePort;
        }

        public int getFirefliesPort() {
            return firefliesPort;
        }

        public int getGhostPort() {
            return ghostPort;
        }

        public String getHostname() {
            return hostname;
        }

        public String getPublicKey() {
            return publicKey;
        }

        public String getSignature() {
            return signature;
        }

        @Override
        public String toString() {
            return "MintRequest [hostname=" + hostname + ", avalanchePort=" + avalanchePort + ", firefliesPort="
                    + firefliesPort + ", ghostPort=" + ghostPort + "]";
        }

    }

    public static class MintResult {
        private String encodedCA;
        private String encodedIdentity;
        private List<String> encodedSeeds;

        public MintResult() {};

        public MintResult(String encodedCA, String encodedIdentity, List<String> encodedSeeds) {
            this.encodedCA = encodedCA;
            this.encodedIdentity = encodedIdentity;
            this.encodedSeeds = encodedSeeds;
        }

        public String getEncodedCA() {
            return encodedCA;
        }

        public String getEncodedIdentity() {
            return encodedIdentity;
        }

        public List<String> getEncodedSeeds() {
            return encodedSeeds;
        }
    }

    private static final Decoder DECODER = Base64.getUrlDecoder();

    private static final Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Logger log = LoggerFactory.getLogger(Bootstrap.class);

    private final X509Certificate ca;
    private final X509Certificate identity;
    private final List<X509Certificate> seeds;

    public Bootstrap(WebTarget targetEndpoint, PublicKey publicKey, Signature s, String hostName, int firefliesPort,
            int ghostPort,
            int avalanchePort, long checkPeriod, int retries) {

        MintRequest request = new MintRequest(hostName, firefliesPort, ghostPort, avalanchePort,
                                              ENCODER.encodeToString(publicKey.getEncoded()),
                                              ENCODER.encodeToString(sign(publicKey, s)));
        log.info("Request: {}", request);

        Response response = null;
        int retry = retries;
        do {
            try {
                response = targetEndpoint.request(MediaType.APPLICATION_JSON)
                                         .post(Entity.json(request));
                break;
            } catch (Throwable e) {
                retry--;
                log.warn("Cannot contact {}, retrying in {} ms (retries left: {} : {})", targetEndpoint.getUri(),
                         checkPeriod, retry, e.toString());
                try {
                    Thread.sleep(checkPeriod);
                } catch (InterruptedException e1) {}
            }
        } while (retry != 0);
        if (response == null) {
            throw new IllegalStateException("Unable to acquire bootstrap certificate from : "
                    + targetEndpoint.getUri());
        }
        if (response.getStatus() != Status.OK.getStatusCode()) {
            throw new IllegalStateException("Invalid response from server when minting: " +
                    response.getStatusInfo());
        }
        MintResult result = response.readEntity(MintResult.class);

        CertificateFactory cf;
        try {
            cf = CertificateFactory.getInstance("X.509");
        } catch (CertificateException e) {
            throw new IllegalStateException("Cannot load X.509 certificate factory", e);
        }
        try {
            ca = (X509Certificate)cf.generateCertificate(new ByteArrayInputStream(DECODER.decode(result.getEncodedCA())));
        } catch (CertificateException e) {
            throw new IllegalStateException("Cannot decode CA certificate", e);
        }
        try {
            identity = (X509Certificate)cf.generateCertificate(new ByteArrayInputStream(DECODER.decode(result.getEncodedIdentity())));
        } catch (CertificateException e) {
            throw new IllegalStateException("Cannot decode Identity certificate", e);
        }

        seeds = result.getEncodedSeeds().stream().map(encoded -> {
            try {
                return (X509Certificate)cf.generateCertificate(new ByteArrayInputStream(DECODER.decode(encoded)));
            } catch (CertificateException e) {
                throw new IllegalStateException("Cannot transform cert: " + e);
            }
        }).collect(Collectors.toList());
    }

    public X509Certificate getCa() {
        return ca;
    }

    public X509Certificate getIdentity() {
        return identity;
    }

    public List<X509Certificate> getSeeds() {
        return seeds;
    }

    private byte[] sign(PublicKey publicKey, Signature s) {
        try {
            s.update(publicKey.getEncoded());
            return s.sign();
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign content", e);
        }
    }

}
