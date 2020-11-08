/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;

import org.junit.jupiter.api.Test;

/**
 * @author hal.hildebrand
 *
 */
public class TestValidator {

    @Test
    public void signing() {

        SecureRandom entropy = new SecureRandom();

        KeyPair consensusKey = Validator.generateKeyPair(2048, "RSA");

        byte[] encoded = consensusKey.getPublic().getEncoded();

        PublicKey test = Validator.publicKeyOf(encoded);
        assertEquals(consensusKey.getPublic(), test);
        byte[] testEncoded = test.getEncoded();
        assertArrayEquals(encoded, testEncoded);
        byte[] signed = Validator.sign(consensusKey.getPrivate(), entropy, encoded);
        assertNotNull(signed);
        assertNotNull(Validator.verify(consensusKey.getPublic(), signed, encoded));
    }
}
