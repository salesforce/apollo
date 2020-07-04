/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.bootstrap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.junit.Test;

import com.salesforce.apollo.bootstrap.MintApi.MintRequest;
import com.salesforce.apollo.bootstrap.MintApi.MintResult;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.github.olivierlemasle.ca.KeysUtil;

/**
 * @author hhildebrand
 */
public class SeedTest {
    private static DropwizardAppExtension<BootstrapConfiguration> EXT     = new DropwizardAppExtension<>(
            BootstrapCA.class, ResourceHelpers.resourceFilePath("seeds.yml"));
    private static final Encoder                                  ENCODER = Base64.getUrlEncoder().withoutPadding();

    @Test
    public void seeds() throws Exception {
        Client client = EXT.client();
        Response response = null;
        for (int i = 0; i < 20; i++) {
            response = mint(client, "localhost", i * 100, i * 101, i * 102);
            assertEquals(200, response.getStatus());
        }

        MintResult result = response.readEntity(MintResult.class);
        assertNotNull(result);
        assertNotNull(result.getEncodedSeeds());

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Decoder decoder = Base64.getUrlDecoder();

        List<Certificate> seeds = result.getEncodedSeeds().stream().map(encoded -> {
            try {
                return cf.generateCertificate(new ByteArrayInputStream(decoder.decode(encoded)));
            } catch (CertificateException e) {
                fail("Cannot transform cert: " + e);
                return null;
            }
        }).collect(Collectors.toList());
        assertEquals(11, seeds.size());
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

    private Response mint(Client client, String host, int fPort, int gPort, int aPort) {

        KeyPair pair = KeysUtil.generateKeyPair();
        MintRequest request = new MintRequest(host, fPort, gPort, aPort,
                ENCODER.encodeToString(pair.getPublic().getEncoded()),
                ENCODER.encodeToString(sign(pair.getPublic(), forSigning(pair.getPrivate(), new SecureRandom()))));
        return client.target(String.format("http://localhost:%d/api/cnc/mint", EXT.getLocalPort()))
                     .request(MediaType.APPLICATION_JSON)
                     .post(Entity.json(request));
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
