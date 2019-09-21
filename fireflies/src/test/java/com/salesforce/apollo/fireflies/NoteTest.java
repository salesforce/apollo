/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.Arrays;
import java.util.BitSet;
import java.util.UUID;

import org.junit.Test;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class NoteTest {

    @Test
    public void serialization() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        KeyPair keyPair = gen.generateKeyPair();

        UUID id = UUID.randomUUID();
        int epoch = 456423456;
        BitSet mask = new BitSet(40);
        mask.set(0);
        mask.set(30);
        mask.set(27);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keyPair.getPrivate(), new SecureRandom());
        Note note = new Note(id, epoch, mask, signature);

        assertEquals(epoch, note.getEpoch());
        assertEquals(id, note.getId());
        BitSet reconstituted = note.getMask();

        assertEquals(mask.toByteArray().length, reconstituted.toByteArray().length);
        assertTrue(Arrays.equals(mask.toByteArray(), reconstituted.toByteArray()));
    }
}
