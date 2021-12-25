package com.salesforce.apollo.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.interfaces.EdECPrivateKey;
import java.security.interfaces.EdECPublicKey;
import java.security.spec.EdECPoint;
import java.security.spec.EdECPrivateKeySpec;
import java.security.spec.EdECPublicKeySpec;
import java.security.spec.NamedParameterSpec;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.utils.Hex;

public class EdDSAOperationsTests {

    @Test
    public void test_ED25519_generateKeyPair() {
        var ops = SignatureAlgorithm.ED_25519;
        var result = ops.generateKeyPair();

        assertEquals("Ed25519", result.getPrivate().getAlgorithm());
        assertEquals("Ed25519", result.getPublic().getAlgorithm());
    }

    @Test
    public void test_ED25519_encode() throws GeneralSecurityException {
        var point = new EdECPoint(false,
                                  new BigInteger("791e61f6798344fc47c1112d1bd53c896b0a5f0fbcae6e3edf70b26c3507f0c",
                                                 16));
        var keyFactory = KeyFactory.getInstance("Ed25519");
        var spec = new EdECPublicKeySpec(NamedParameterSpec.ED25519, point);
        var publicKey = (EdECPublicKey) keyFactory.generatePublic(spec);

        var ops = SignatureAlgorithm.ED_25519;
        var result = ops.encode(publicKey);

        var expected = Hex.unhex("0c7f50c3260bf7ede3e6cafbf0a5b096c853bdd112117cc44f3498671fe69107");
        assertArrayEquals(expected, result);
    }

    @Test
    public void test_ED25519_decode() {
        var encoded = Hex.unhex("0c7f50c3260bf7ede3e6cafbf0a5b096c853bdd112117cc44f3498671fe69107");

        var ops = SignatureAlgorithm.ED_25519;
        var result = (EdECPublicKey) ops.publicKey(encoded);

        assertEquals("Ed25519", result.getAlgorithm());
        assertEquals(NamedParameterSpec.ED25519.getName(), result.getParams().getName());

        var expectedPoint = new EdECPoint(false,
                                          new BigInteger("791e61f6798344fc47c1112d1bd53c896b0a5f0fbcae6e3edf70b26c3507f0c",
                                                         16));
        assertEquals(expectedPoint.isXOdd(), result.getPoint().isXOdd());
        assertEquals(expectedPoint.getY(), result.getPoint().getY());
    }

    @Test
    public void test_ED25519_encodeDecodeRoundtrip() throws GeneralSecurityException {
        final var ecPointY = new BigInteger("791e61f6798344fc47c1112d1bd53c896b0a5f0fbcae6e3edf70b26c3507f0c", 16);
        var point = new EdECPoint(false, ecPointY);
        var keyFactory = KeyFactory.getInstance("Ed25519");
        var spec = new EdECPublicKeySpec(NamedParameterSpec.ED25519, point);
        var publicKey = (EdECPublicKey) keyFactory.generatePublic(spec);

        var ops = SignatureAlgorithm.ED_25519;

        var bytes = ops.encode(publicKey);
        var decoding = (EdECPublicKey) ops.publicKey(bytes);

        assertEquals("Ed25519", decoding.getAlgorithm());
        assertEquals(NamedParameterSpec.ED25519.getName(), decoding.getParams().getName());

        var expectedPoint = new EdECPoint(false, ecPointY);
        assertEquals(expectedPoint.isXOdd(), decoding.getPoint().isXOdd());
        assertEquals(expectedPoint.getY(), decoding.getPoint().getY());
    }

    @Test
    public void test_ED25519_decodeEncodeRoundtrip() {
        var encoded = Hex.unhex("0c7f50c3260bf7ede3e6cafbf0a5b096c853bdd112117cc44f3498671fe69107");

        var ops = SignatureAlgorithm.ED_25519;
        var publicKey = ops.publicKey(encoded);
        var bytes = ops.encode(publicKey);

        assertArrayEquals(encoded, bytes);
    }

    @Test
    public void test_ED25519_signVerify() throws GeneralSecurityException {
        var skb = Hex.unhex("4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb");
        var pkb = Hex.unhex("3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c");
        var msg = Hex.unhex("72");
        var expectedSig = Hex.unhex("92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da"
        + "085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00");

        var privateKeySpec = new EdECPrivateKeySpec(NamedParameterSpec.ED25519, skb);
        var kf = KeyFactory.getInstance("EdDSA");
        var privateKey = (EdECPrivateKey) kf.generatePrivate(privateKeySpec);

        var ops = SignatureAlgorithm.ED_25519;
        var sig = ops.sign(privateKey, msg);

        assertArrayEquals(expectedSig, sig.getBytes()[0]);

        var publicKey = ops.publicKey(pkb);
        assertTrue(ops.verify(publicKey, sig, msg));
    }

    @Test
    public void test_ED448_generateKeyPair() {
        var ops = SignatureAlgorithm.ED_448;
        var result = ops.generateKeyPair();

        assertEquals("Ed448", result.getPrivate().getAlgorithm());
        assertEquals("Ed448", result.getPublic().getAlgorithm());
    }

    @Test
    public void test_ED448_encode() throws GeneralSecurityException {
        var point = new EdECPoint(true,
                                  new BigInteger("c5063dca640dca6f3db3b385626db1e6fc4265648de7d83f79a2fcf0db04a8f53796daeb18c3d622db05bd729945f14421f1b84b6af39baf",
                                                 16));
        var keyFactory = KeyFactory.getInstance("Ed448");
        var spec = new EdECPublicKeySpec(NamedParameterSpec.ED448, point);
        var publicKey = (EdECPublicKey) keyFactory.generatePublic(spec);

        var ops = SignatureAlgorithm.ED_448;
        var result = ops.encode(publicKey);

        var expected = Hex.unhex("af9bf36a4bb8f12144f1459972bd05db22d6c318ebda9637f5a804dbf0fca2793fd8e78d646542fce6b16d6285b3b33d6fca0d64ca3d06c580");
        assertArrayEquals(expected, result);
    }

    @Test
    public void test_ED448_decode() {
        var encoded = Hex.unhex("af9bf36a4bb8f12144f1459972bd05db22d6c318ebda9637f5a804dbf0fca2793fd8e78d646542fce6b16d6285b3b33d6fca0d64ca3d06c580");

        var ops = SignatureAlgorithm.ED_448;
        var result = (EdECPublicKey) ops.publicKey(encoded);

        assertEquals("Ed448", result.getAlgorithm());
        assertEquals(NamedParameterSpec.ED448.getName(), result.getParams().getName());

        var expectedPoint = new EdECPoint(true,
                                          new BigInteger("c5063dca640dca6f3db3b385626db1e6fc4265648de7d83f79a2fcf0db04a8f53796daeb18c3d622db05bd729945f14421f1b84b6af39baf",
                                                         16));
        assertEquals(expectedPoint.isXOdd(), result.getPoint().isXOdd());
        assertEquals(expectedPoint.getY(), result.getPoint().getY());
    }

    @Test
    public void test_ED448_encodeDecodeRoundtrip() throws GeneralSecurityException {
        final var ecPointY = new BigInteger("c5063dca640dca6f3db3b385626db1e6fc4265648de7d83f79a2fcf0db04a8f53796daeb18c3d622db05bd729945f14421f1b84b6af39baf",
                                            16);
        var point = new EdECPoint(true, ecPointY);
        var keyFactory = KeyFactory.getInstance("Ed448");
        var spec = new EdECPublicKeySpec(NamedParameterSpec.ED448, point);
        var publicKey = (EdECPublicKey) keyFactory.generatePublic(spec);

        var ops = SignatureAlgorithm.ED_448;

        var bytes = ops.encode(publicKey);
        var decoding = (EdECPublicKey) ops.publicKey(bytes);

        assertEquals("Ed448", decoding.getAlgorithm());
        assertEquals(NamedParameterSpec.ED448.getName(), decoding.getParams().getName());

        var expectedPoint = new EdECPoint(true, ecPointY);
        assertEquals(expectedPoint.isXOdd(), decoding.getPoint().isXOdd());
        assertEquals(expectedPoint.getY(), decoding.getPoint().getY());
    }

    @Test
    public void test_ED448_decodeEncodeRoundtrip() {
        var encoded = Hex.unhex("f35ec70547678147cd6fc0bec2e31a35b37adfba1fee705372f3586ceea6a82236d32a2da37eaca4e30b6e3e6926c1de98d4583cfcff243500");

        var ops = SignatureAlgorithm.ED_448;
        var publicKey = ops.publicKey(encoded);
        var bytes = ops.encode(publicKey);

        assertArrayEquals(encoded, bytes);
    }

    @Test
    public void test_ED448_signVerify() throws GeneralSecurityException {
        var skb = Hex.unhex("61768c7a6fb18b9c257e72fdc8f4e40c62dec41380ec87f30f1865e156838c7694b7ce53dd6882d882559447fde7fcc86f3928fa8d8ac81b22");
        var pkb = Hex.unhex("af9bf36a4bb8f12144f1459972bd05db22d6c318ebda9637f5a804dbf0fca2793fd8e78d646542fce6b16d6285b3b33d6fca0d64ca3d06c580");
        var msg = Hex.unhex("72");
        var expectedSig = Hex.unhex("c969c2fc411332b79e91a8e39a18749fea17e1b1676814aca11de2990db1234807299dfb6e95514811d23264edafe42a4afc9b447d7ef1c000c2d9c5c65ec14c3f65acc66d2abb57996c11c252acccce76841a3b7bc8fd18cdb3073ece73d7cfb12d3fc2f51c68e5d6f1fd6a980111a00000");

        var privateKeySpec = new EdECPrivateKeySpec(NamedParameterSpec.ED448, skb);
        var kf = KeyFactory.getInstance("EdDSA");
        var privateKey = (EdECPrivateKey) kf.generatePrivate(privateKeySpec);

        var ops = SignatureAlgorithm.ED_448;
        var sig = ops.sign(privateKey, msg);

        assertArrayEquals(expectedSig, sig.getBytes()[0]);

        var publicKey = ops.publicKey(pkb);
        assertTrue(ops.verify(publicKey, sig, msg));
    }

}
