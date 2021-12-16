package com.salesforce.apollo.stereotomy.specification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;

public class IdentifierSpecificationTests {

    SecureRandom deterministicRandom;
    KeyPair      keyPair;
    KeyPair      keyPair2;
    Signer       signer;

    @BeforeEach
    public void beforeEachTest() throws NoSuchAlgorithmException {
        // this makes the values of secureRandom deterministic
        this.deterministicRandom = SecureRandom.getInstance("SHA1PRNG");
        this.deterministicRandom.setSeed(new byte[] { 0 });

        this.keyPair = SignatureAlgorithm.ED_25519.generateKeyPair();
        this.signer = new SignerImpl(this.keyPair.getPrivate());

        this.keyPair2 = SignatureAlgorithm.ED_25519.generateKeyPair();

    }

    @Test
    public void testBuilderSigningThresholdInt() {
        var spec = IdentifierSpecification.newBuilder()
                                          .setKeys(Arrays.asList(this.keyPair.getPublic(), keyPair2.getPublic()))
                                          .setNextKeys(Arrays.asList(this.keyPair.getPublic(), keyPair2.getPublic()))
                                          .setSigner(this.signer).setSigningThreshold(1).build();

        assertTrue(spec.getSigningThreshold() instanceof SigningThreshold.Unweighted);
        assertEquals(1, ((SigningThreshold.Unweighted) spec.getSigningThreshold()).getThreshold());
    }

    @Test
    public void testBuilderSigningThresholdUnweighted() {
        var spec = IdentifierSpecification.newBuilder().addKey(this.keyPair.getPublic()).setSigner(this.signer)
                                          .setNextKeys(Arrays.asList(keyPair2.getPublic()))
                                          .setNextSigningThreshold(SigningThreshold.unweighted(1)).build();

        assertTrue(spec.getSigningThreshold() instanceof SigningThreshold.Unweighted);
        assertEquals(1, ((SigningThreshold.Unweighted) spec.getSigningThreshold()).getThreshold());
    }

    @Test
    public void testBuilderSigningThresholdWeighted() {
        var spec = IdentifierSpecification.newBuilder()
                                          .setKeys(Arrays.asList(this.keyPair.getPublic(), this.keyPair2.getPublic()))
                                          .setNextKeys(Arrays.asList(this.keyPair.getPublic(),
                                                                     this.keyPair2.getPublic()))
                                          .setSigner(this.signer)
                                          .setSigningThreshold(SigningThreshold.weighted("1", "2")).build();

        SigningThreshold signingThreshold = spec.getSigningThreshold();
        assertTrue(signingThreshold instanceof SigningThreshold.Weighted);
        var weights = ((SigningThreshold.Weighted) signingThreshold).getWeights();
        assertEquals(SigningThreshold.weight(1), weights[0][0]);
        assertEquals(SigningThreshold.weight(2), weights[0][1]);
    }

}
