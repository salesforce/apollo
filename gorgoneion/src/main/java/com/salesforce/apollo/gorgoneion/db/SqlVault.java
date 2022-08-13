/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion.db;

import static com.salesforce.apollo.gorgoneion.schema.Tables.PASSWORDS;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.crypto.generators.SCrypt;
import org.bouncycastle.crypto.util.ScryptConfig;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.gorgoneion.Vault;

/**
 * @author hal.hildebrand
 *
 */
public class SqlVault implements Vault {

    private final static int N = 16384; // Cost parameter
    private final static int P = 1;     // Parallelization factor
    private final static int R = 8;     // Block size

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private final SignatureAlgorithm algorithm;
    private final DSLContext         context;
    private final SecureRandom       entropy;
    private final int                keyLength = 16; // 16-byte output from scrypt

    private final ScryptConfig scrypt;

    public SqlVault(DSLContext dsl, SecureRandom entropy) {
        this(dsl, entropy, new ScryptConfig.Builder(N, R, P).build(), SignatureAlgorithm.DEFAULT);
    }

    public SqlVault(DSLContext dsl, SecureRandom entropy, ScryptConfig scrypt, SignatureAlgorithm algorithm) {
        this.context = dsl;
        this.entropy = entropy;
        this.scrypt = scrypt;
        this.algorithm = algorithm;
    }

    @Override
    public void add(String name, byte[] password, boolean admin) {
        var passwordSalt = new byte[keyLength];
        var keySalt = new byte[keyLength];
        var iv = new byte[keyLength];

        entropy.nextBytes(passwordSalt);
        entropy.nextBytes(keySalt);
        entropy.nextBytes(iv);

        var passKey = derivePasswordKey(password, keySalt);
        var keyPair = algorithm.generateKeyPair(entropy);
        IvParameterSpec ivParams = new IvParameterSpec(iv);
        SecretKeySpec skeySpec = new SecretKeySpec(passKey, "AES");

        Cipher cipher;
        try {
            cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new IllegalStateException(e);
        }
        try {
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, ivParams);
        } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new IllegalStateException(e);
        }

        byte[] encrypted;
        try {
            encrypted = cipher.doFinal(keyPair.getPrivate().getEncoded());
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new IllegalStateException(e);
        }
        context.transaction(ctx -> {
            var dsl = DSL.using(ctx);

            var record = dsl.newRecord(PASSWORDS);

            // Name
            record.setUsername(name);

            // Salts n IV
            record.setPasswordsalt(passwordSalt);
            record.setKeysalt(keySalt);
            record.setIv(iv);

            // Hashed password
            record.setHashedpassword(hashPassword(password, passwordSalt));

            // Public key
            record.setPublickey(keyPair.getPublic().getEncoded());

            // Encrypted key
            record.setPrivatekey(encrypted);

            record.setAdmin(admin);
            record.insert();
        });
    }

    @Override
    public void changePassword(String name, byte[] password, byte[] newPassword) {
        var passwordSalt = new byte[keyLength];
        var keySalt = new byte[keyLength];
        var iv = new byte[keyLength];

        entropy.nextBytes(passwordSalt);
        entropy.nextBytes(keySalt);
        entropy.nextBytes(iv);

        context.transaction(ctx -> {
            var dsl = DSL.using(ctx);

            var record = dsl.newRecord(PASSWORDS);
            record.setUsername(name);
            record.update();

            // Decrypt private key with current password and IV
            var privateKey = decrypt(record.getPrivatekey(), new IvParameterSpec(record.getIv()), password,
                                     record.getKeysalt());

            // Derive new password key
            var passKey = derivePasswordKey(newPassword, keySalt);

            // New IV
            IvParameterSpec ivParams = new IvParameterSpec(iv);

            // Reencrypt
            SecretKeySpec skeySpec = new SecretKeySpec(passKey, "AES");
            Cipher cipher;
            try {
                cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
                throw new IllegalStateException(e);
            }
            try {
                cipher.init(Cipher.ENCRYPT_MODE, skeySpec, ivParams);
            } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
                throw new IllegalStateException(e);
            }

            byte[] encrypted;
            try {
                encrypted = cipher.doFinal(privateKey);
            } catch (IllegalBlockSizeException | BadPaddingException e) {
                throw new IllegalStateException(e);
            }

            // Salts n IV
            record.setPasswordsalt(passwordSalt);
            record.setKeysalt(keySalt);
            record.setIv(iv);

            // Hashed password
            record.setHashedpassword(hashPassword(newPassword, passwordSalt));

            // Encrypted key
            record.setPrivatekey(encrypted);

            record.merge();
        });
    }

    @Override
    public boolean contains(String name) {
        return context.transactionResult(ctx -> {
            var dsl = DSL.using(ctx);
            return dsl.fetchExists(dsl.select(PASSWORDS.ID).from(PASSWORDS).where(PASSWORDS.USERNAME.eq(name)));
        });
    }

    private byte[] decrypt(byte[] privatekey, IvParameterSpec iv, byte[] password, byte[] keySalt) {
        var passKey = derivePasswordKey(password, keySalt);
        SecretKeySpec skeySpec = new SecretKeySpec(passKey, "AES");
        Cipher cipher;
        try {
            cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
            throw new IllegalStateException(e);
        }
        try {
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
        } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new IllegalStateException(e);
        }

        byte[] decrypted;
        try {
            decrypted = cipher.doFinal(privatekey);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new IllegalStateException(e);
        }
        return decrypted;
    }

    private byte[] derivePasswordKey(byte[] password, byte[] keySalt) {
        return SCrypt.generate(password, keySalt, scrypt.getCostParameter(), scrypt.getBlockSize(),
                               scrypt.getParallelizationParameter(), keyLength);
    }

    private byte[] hashPassword(byte[] password, byte[] passwordSalt) {
        return SCrypt.generate(password, passwordSalt, scrypt.getCostParameter(), scrypt.getBlockSize(),
                               scrypt.getParallelizationParameter(), keyLength);
    }

}
