package com.salesforce.apollo.cryptography;

import org.bouncycastle.jcajce.SecretKeyWithEncapsulation;
import org.bouncycastle.jcajce.spec.KEMExtractSpec;
import org.bouncycastle.jcajce.spec.KEMGenerateSpec;
import org.bouncycastle.pqc.jcajce.provider.BouncyCastlePQCProvider;
import org.bouncycastle.pqc.jcajce.spec.KyberParameterSpec;
import org.bouncycastle.util.Arrays;

import javax.crypto.KeyGenerator;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

/**
 * @author hal.hildebrand
 **/
public class KyberKemTest {

    public static void main(String[] args) {
        // Security.addProvider(new BouncyCastleProvider());
        // we do need the regular Bouncy Castle file that includes the PQC provider
        // get Bouncy Castle here: https://mvnrepository.com/artifact/org.bouncycastle/bcprov-jdk18on
        // tested with BC version 1.72
        if (Security.getProvider("BCPQC") == null) {
            Security.addProvider(new BouncyCastlePQCProvider());
        }
        String print = run(false);
        System.out.println(print);
    }

    public static String run(boolean truncateKeyOutput) {
        String out = "PQC Chrystals-Kyber KEM";
        out += "\n" + "\n************************************\n" + "* # # SERIOUS SECURITY WARNING # # *\n"
        + "* This program is a CONCEPT STUDY  *\n" + "* for the algorithm                *\n"
        + "* Chrystals-Kyber [key exchange    *\n" + "* mechanism]                       *\n"
        + "* The program is using an          *\n" + "* parameter set that I cannot      *\n"
        + "* check for the correctness of the *\n" + "* output and other details         *\n"
        + "*                                  *\n" + "*    DO NOT USE THE PROGRAM IN     *\n"
        + "*    ANY PRODUCTION ENVIRONMENT    *\n" + "************************************";

        // as there are 3 parameter sets available the program runs all of them
        KyberParameterSpec[] kyberParameterSpecs = { KyberParameterSpec.kyber512, KyberParameterSpec.kyber768,
                                                     KyberParameterSpec.kyber1024 };

        // statistics
        int nrOfSpecs = kyberParameterSpecs.length;
        String[] parameterSpecName = new String[nrOfSpecs];
        int[] privateKeyLength = new int[nrOfSpecs];
        int[] publicKeyLength = new int[nrOfSpecs];
        int[] encryptionKeyLength = new int[nrOfSpecs];
        int[] encapsulatedKeyLength = new int[nrOfSpecs];
        boolean[] encryptionKeysEquals = new boolean[nrOfSpecs];

        out += "\n\n****************************************\n";
        for (int i = 0; i < nrOfSpecs; i++) {
            // generation of the Chrystals-Kyber key pair
            KyberParameterSpec kyberParameterSpec = kyberParameterSpecs[i];
            String kyberParameterSpecName = kyberParameterSpec.getName();
            parameterSpecName[i] = kyberParameterSpecName;
            out += "\n" + "Chrystals-Kyber KEM with parameterset " + kyberParameterSpecName;
            KeyPair keyPair = generateChrystalsKyberKeyPair(kyberParameterSpec);

            // get private and public key
            PrivateKey privateKey = keyPair.getPrivate();
            PublicKey publicKey = keyPair.getPublic();

            // storing the key as byte array
            byte[] privateKeyByte = privateKey.getEncoded();
            byte[] publicKeyByte = publicKey.getEncoded();
            out += "\n" + "\ngenerated private key length: " + privateKeyByte.length;
            out += "\n" + "generated public key length:  " + publicKeyByte.length;
            privateKeyLength[i] = privateKeyByte.length;
            publicKeyLength[i] = publicKeyByte.length;

            // generate the keys from a byte array
            PrivateKey privateKeyLoad = getChrystalsKyberPrivateKeyFromEncoded(privateKeyByte);
            PublicKey publicKeyLoad = getChrystalsKyberPublicKeyFromEncoded(publicKeyByte);

            // generate the encryption key and the encapsulated key
            out += "\n" + "\nEncryption side: generate the encryption key and the encapsulated key";
            SecretKeyWithEncapsulation secretKeyWithEncapsulationSender = pqcGenerateChrystalsKyberEncryptionKey(
            publicKeyLoad);
            byte[] encryptionKey = secretKeyWithEncapsulationSender.getEncoded();
            out += "\n" + "encryption key length: " + encryptionKey.length + " key: " + bytesToHex(
            secretKeyWithEncapsulationSender.getEncoded());
            byte[] encapsulatedKey = secretKeyWithEncapsulationSender.getEncapsulation();
            out +=
            "\n" + "encapsulated key length: " + encapsulatedKey.length + " key: " + (truncateKeyOutput ? shortenString(
            bytesToHex(encapsulatedKey)) : bytesToHex(encapsulatedKey));

            encryptionKeyLength[i] = encryptionKey.length;
            encapsulatedKeyLength[i] = encapsulatedKey.length;

            out += "\n" + "\nDecryption side: receive the encapsulated key and generate the decryption key";
            byte[] decryptionKey = pqcGenerateChrystalsKyberDecryptionKey(privateKeyLoad, encapsulatedKey);
            out += "\n" + "decryption key length: " + decryptionKey.length + " key: " + bytesToHex(decryptionKey);
            boolean keysAreEqual = Arrays.areEqual(encryptionKey, decryptionKey);
            out += "\n" + "decryption key is equal to encryption key: " + keysAreEqual;
            encryptionKeysEquals[i] = keysAreEqual;
            out += "\n\n****************************************\n";
        }

        out += "\n" + "Test results";
        out += "\n" + "parameter spec name  priKL   pubKL encKL capKL  keyE" + "\n";
        for (int i = 0; i < nrOfSpecs; i++) {
            String out1 = String.format("%-20s%6d%8d%6d%6d%6b%n", parameterSpecName[i], privateKeyLength[i],
                                        publicKeyLength[i], encryptionKeyLength[i], encapsulatedKeyLength[i],
                                        encryptionKeysEquals[i]);
            out += out1;
        }
        out += "\n" + "Legend: priKL privateKey length, pubKL publicKey length, encKL encryption key length, "
        + "capKL encapsulated key length" + "\n";
        out += "****************************************\n";
        return out;
    }

    private static String shortenString(String input) {
        if (input != null && input.length() > 32) {
            return input.substring(0, 32) + " ...";
        } else {
            return input;
        }
    }

    private static KeyPair generateChrystalsKyberKeyPair(KyberParameterSpec kyberParameterSpec) {
        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("KYBER", "BCPQC");
            kpg.initialize(kyberParameterSpec, new SecureRandom());
            KeyPair kp = kpg.generateKeyPair();
            return kp;
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidAlgorithmParameterException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static SecretKeyWithEncapsulation pqcGenerateChrystalsKyberEncryptionKey(PublicKey publicKey) {
        KeyGenerator keyGen = null;
        try {
            keyGen = KeyGenerator.getInstance("KYBER", "BCPQC");
            keyGen.init(new KEMGenerateSpec(publicKey, "AES"), new SecureRandom());
            SecretKeyWithEncapsulation secEnc1 = (SecretKeyWithEncapsulation) keyGen.generateKey();
            return secEnc1;
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidAlgorithmParameterException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static byte[] pqcGenerateChrystalsKyberDecryptionKey(PrivateKey privateKey, byte[] encapsulatedKey) {
        KeyGenerator keyGen = null;
        try {
            keyGen = KeyGenerator.getInstance("KYBER", "BCPQC");
            keyGen.init(new KEMExtractSpec(privateKey, encapsulatedKey, "AES"), new SecureRandom());
            SecretKeyWithEncapsulation secEnc2 = (SecretKeyWithEncapsulation) keyGen.generateKey();
            return secEnc2.getEncoded();
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidAlgorithmParameterException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static PrivateKey getChrystalsKyberPrivateKeyFromEncoded(byte[] encodedKey) {
        PKCS8EncodedKeySpec pkcs8EncodedKeySpec = new PKCS8EncodedKeySpec(encodedKey);
        KeyFactory keyFactory = null;
        try {
            keyFactory = KeyFactory.getInstance("KYBER", "BCPQC");
            return keyFactory.generatePrivate(pkcs8EncodedKeySpec);
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeySpecException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static PublicKey getChrystalsKyberPublicKeyFromEncoded(byte[] encodedKey) {
        X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(encodedKey);
        try {
            KeyFactory keyFactory = KeyFactory.getInstance("KYBER", "BCPQC");
            return keyFactory.generatePublic(x509EncodedKeySpec);
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeySpecException e) {
            e.printStackTrace();
            return null;
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuffer result = new StringBuffer();
        for (byte b : bytes)
            result.append(Integer.toString((b & 0xff) + 0x100, 16).substring(1));
        return result.toString();
    }
}
