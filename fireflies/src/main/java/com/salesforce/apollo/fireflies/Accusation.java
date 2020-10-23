/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import static com.salesforce.apollo.protocols.Conversion.hashOf;

import java.nio.ByteBuffer;
import java.security.Signature;
import java.security.SignatureException;

import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class Accusation implements Verifiable {
    private static final int ACCUSED_INDEX;
    private static final int ACCUSER_INDEX;
    private static final int EPOCH_INDEX = 0;
    private static final int RING_INDEX;
    private static final int SIZE        = 8 + 32 + 32 + 4;

    static {
        ACCUSER_INDEX = EPOCH_INDEX + 8;
        ACCUSED_INDEX = ACCUSER_INDEX + 32;
        RING_INDEX = ACCUSED_INDEX + 32;
    }

    private final byte[] content;
    private final byte[] hash;
    private final byte[] signature;

    public Accusation(byte[] content, byte[] signature) {
        this.content = content;
        this.signature = signature;
        hash = hashOf(content);
    }

    public Accusation(long epoch, HashKey accuser, int ringNumber, HashKey accused, Signature s) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        buffer.putLong(epoch);
        accuser.write(buffer);
        accused.write(buffer);
        buffer.putInt(ringNumber);
        content = buffer.array();
        hash = hashOf(content);
        try {
            s.update(content);
            signature = s.sign();
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign content", e);
        }
    }

    @Override
    public byte[] content() {
        return content;
    }

    /**
     * The identity of the accused
     */
    public HashKey getAccused() {
        ByteBuffer buffer = ByteBuffer.wrap(content);
        byte[] buf = new byte[32];
        buffer.position(ACCUSED_INDEX);
        buffer.get(buf, 0, 32);
        return new HashKey(buf);
    }

    /**
     * The identity of the accuser
     */
    public HashKey getAccuser() {
        ByteBuffer buffer = ByteBuffer.wrap(content);
        byte[] buf = new byte[32];
        buffer.position(ACCUSER_INDEX);
        buffer.get(buf, 0, 32);
        return new HashKey(buf);
    }

    public byte[] getContent() {
        return content;
    }

    /**
     * The epoch of the accused note
     */
    public long getEpoch() {
        ByteBuffer buffer = ByteBuffer.wrap(content);
        return buffer.getLong(EPOCH_INDEX);
    }

    /**
     * The ring number on which the accusation was made
     */
    public int getRingNumber() {
        ByteBuffer buffer = ByteBuffer.wrap(content);
        return buffer.getInt(RING_INDEX);
    }

    @Override
    public byte[] getSignature() {
        return signature;
    }

    @Override
    public byte[] hash() {
        return hash;
    }
}
