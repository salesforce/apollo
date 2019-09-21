/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static org.junit.Assert.assertEquals;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.UUID;

import org.junit.Test;

public class AccusationTest {

    @Test
    public void serialization() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        KeyPair keyPair = gen.generateKeyPair();

        UUID accuser = UUID.randomUUID();
        UUID accused = UUID.randomUUID();
        int epoch = 456423456;
        int ringNumber = 17;

        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keyPair.getPrivate(), new SecureRandom());

        Accusation accusation = new Accusation(epoch, accuser, ringNumber, accused, signature);

        assertEquals(epoch, accusation.getEpoch());
        assertEquals(accuser, accusation.getAccuser());
        assertEquals(accused, accusation.getAccused());
        assertEquals(ringNumber, accusation.getRingNumber());
    }
}
