package com.salesforce.apollo.stereotomy.specification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;

public class IdentifierSpecificationTests {

    SecureRandom deterministicRandom;
    KeyPair      keyPair;
    KeyPair      keyPair2;
    Signer       signer;
    Signer       signer2;

    @BeforeEach
    public void beforeEachTest() throws NoSuchAlgorithmException {
        // this makes the values of secureRandom deterministic
        this.deterministicRandom = SecureRandom.getInstance("SHA1PRNG");
        this.deterministicRandom.setSeed(new byte[] { 0 });

        this.keyPair = SignatureAlgorithm.ED_25519.generateKeyPair();
        this.signer = new SignerImpl(1, this.keyPair.getPrivate());

        this.keyPair2 = SignatureAlgorithm.ED_25519.generateKeyPair();
        this.signer2 = new SignerImpl(2, this.keyPair2.getPrivate());

    }

    @Test
    public void test__builder__signingThreshold__int() {
        var spec = IdentifierSpecification.newBuilder()
                                          .setKey(this.keyPair.getPublic())
                                          .setSigner(this.signer)
                                          .setSigningThreshold(1)
                                          .build();

        assertTrue(spec.getSigningThreshold() instanceof SigningThreshold.Unweighted);
        assertEquals(1, ((SigningThreshold.Unweighted) spec.getSigningThreshold()).getThreshold());
    }

    @Test
    public void test__builder__signingThreshold__unweighted() {
        var spec = IdentifierSpecification.newBuilder()
                                          .setKey(this.keyPair.getPublic())
                                          .setSigner(this.signer)
                                          .setNextSigningThreshold(SigningThreshold.unweighted(1))
                                          .build();

        assertTrue(spec.getSigningThreshold() instanceof SigningThreshold.Unweighted);
        assertEquals(1, ((SigningThreshold.Unweighted) spec.getSigningThreshold()).getThreshold());
    }

    @Test
    public void test__builder__signingThreshold__weighted() {
        var spec = IdentifierSpecification.newBuilder()
                                          .setKey(this.keyPair.getPublic())
                                          .setKey(this.keyPair2.getPublic())
                                          .setSigner(this.signer)
                                          .setSigningThreshold(SigningThreshold.weighted("1", "2"))
                                          .build();

        SigningThreshold signingThreshold = spec.getSigningThreshold();
        assertTrue(signingThreshold instanceof SigningThreshold.Weighted);
        var weights = ((SigningThreshold.Weighted) signingThreshold).getWeights();
        assertEquals(SigningThreshold.weight(1), weights[0][0]);
        assertEquals(SigningThreshold.weight(2), weights[0][1]);
    }

}
