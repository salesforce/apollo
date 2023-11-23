package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 **/
public class VerifierTest {
    @Test
    public void stereotomy() throws Exception {
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        var identifier = stereotomy.newIdentifier();
        var testMsg = "Give me food or give me slack or kill me";
        var signature1 = identifier.getSigner().sign(testMsg.getBytes());
        final var verifier = identifier.getVerifier().get();
        assertTrue(verifier.verify(signature1, testMsg.getBytes()));

        identifier.rotate();
        identifier.rotate();

        assertTrue(
        verifier.verify(signature1, testMsg.getBytes())); // kerl verifier knows what key to use to verify the signature

        final var signature2 = identifier.getSigner().sign(testMsg.getBytes());
        assertTrue(verifier.verify(signature2, testMsg.getBytes()));

        identifier.rotate();

        final var signature3 = identifier.getSigner().sign(testMsg.getBytes());
        assertTrue(verifier.verify(signature1, testMsg.getBytes()));
        assertTrue(verifier.verify(signature3, testMsg.getBytes()));
    }
}
