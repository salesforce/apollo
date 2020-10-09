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
import org.junit.jupiter.api.extension.ExtendWith;

import com.salesforce.apollo.bootstrap.MintApi.MintRequest;
import com.salesforce.apollo.bootstrap.MintApi.MintResult;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.github.olivierlemasle.ca.KeysUtil;

/**
 * @author hhildebrand
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class BoostrapCaTest {
    private static DropwizardAppExtension<BootstrapConfiguration> EXT     = new DropwizardAppExtension<>(
            BootstrapCA.class, ResourceHelpers.resourceFilePath("server.yml"));
    private static final Encoder                                  ENCODER = Base64.getUrlEncoder().withoutPadding();

    @Test
    public void smoke() throws Exception {
        Client client = EXT.client();
        final KeyPair pair = KeysUtil.generateKeyPair();
        MintRequest request = new MintRequest("localhost", 8, 9, 10,
                ENCODER.encodeToString(pair.getPublic().getEncoded()),
                ENCODER.encodeToString(sign(pair.getPublic(), forSigning(pair.getPrivate(), new SecureRandom()))));
        Response response = client.target(String.format("http://localhost:%d/api/cnc/mint", EXT.getLocalPort()))
                                  .request(MediaType.APPLICATION_JSON)
                                  .post(Entity.json(request));
        assertEquals(200, response.getStatus());
        MintResult result = response.readEntity(MintResult.class);
        assertNotNull(result);
        assertNotNull(result.getEncodedIdentity());
        assertNotNull(result.getEncodedCA());
        assertNotNull(result.getEncodedSeeds());

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Decoder decoder = Base64.getUrlDecoder();

        Certificate ca = cf.generateCertificate(new ByteArrayInputStream(decoder.decode(result.getEncodedCA())));
        assertNotNull(ca);
        Certificate identity = cf.generateCertificate(new ByteArrayInputStream(
                decoder.decode(result.getEncodedIdentity())));
        assertNotNull(identity);
        List<Certificate> seeds = result.getEncodedSeeds().stream().map(encoded -> {
            try {
                return cf.generateCertificate(new ByteArrayInputStream(decoder.decode(encoded)));
            } catch (CertificateException e) {
                fail("Cannot transform cert: " + e);
                return null;
            }
        }).collect(Collectors.toList());
        assertEquals(0, seeds.size());
        assertEquals(pair.getPublic(), identity.getPublicKey());
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

    private byte[] sign(PublicKey publicKey, Signature s) {
        try {
            s.update(publicKey.getEncoded());
            return s.sign();
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign content", e);
        }
    }
}
