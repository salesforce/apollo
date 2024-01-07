package com.salesforce.apollo.cryptography;

import org.junit.jupiter.api.Test;

import javax.crypto.KeyAgreement;
import java.security.SecureRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author hal.hildebrand
 **/
public class XTest {
    @Test
    public void testEncoding() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        var algorithm = EncryptionAlgorithm.X_25519;
        var pair = algorithm.generateKeyPair(entropy);
        assertNotNull(pair);
        var encodedPublic = algorithm.encode(pair.getPublic());
        assertNotNull(encodedPublic);
        var decodedPublic = algorithm.publicKey(encodedPublic);
        assertNotNull(decodedPublic);
        assertEquals(pair.getPublic(), decodedPublic);
    }

    @Test
    public void testRoundTrip() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        var algorithm = EncryptionAlgorithm.X_25519;
        var pair1 = algorithm.generateKeyPair(entropy);
        assertNotNull(pair1);
        var pair2 = algorithm.generateKeyPair(entropy);
        assertNotNull(pair2);

        KeyAgreement ka = KeyAgreement.getInstance("XDH");
        KeyAgreement ka2 = KeyAgreement.getInstance("XDH");

        ka.init(pair1.getPrivate());
        ka2.init(pair2.getPrivate());

        ka.doPhase(pair2.getPublic(), true);
        ka2.doPhase(pair1.getPublic(), true);

        byte[] secret1 = ka.generateSecret();
        assertNotNull(secret1);
        byte[] secret2 = ka2.generateSecret();
        assertNotNull(secret2);

        assertArrayEquals(secret1, secret2);
    }
}
