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
import java.util.BitSet;

import com.salesforce.apollo.protocols.HashKey;

/**
 * Members issue notes to signal to other members that they are alive. In
 * particular, a member will issue a new note as a rebuttal to a false
 * accusation. Notes are immutable after they are signed.
 * 
 * @author hal.hildebrand
 * @since 218
 */
public class Note implements Verifiable {
    private static final int BASE_SIZE   = 8 + 32;
    private static final int EPOCH_INDEX = 0;
    private static final int ID_INDEX;
    private static final int MASK_INDEX;

    static {
        ID_INDEX = EPOCH_INDEX + 8;
        MASK_INDEX = ID_INDEX + 32;
    }

    /**
     * Binary content of the note.
     */
    private final byte[] content;

    private final byte[] hash;

    /**
     * Signed with the member's private key.
     */
    private final byte[] signature;

    public Note(byte[] content, byte[] signature) {
        this.content = content;
        this.signature = signature;
        hash = hashOf(content);
    }

    public Note(HashKey id, long epoch, BitSet mask, Signature s) {
        byte[] maskBytes = mask.toByteArray();
        ByteBuffer buffer = ByteBuffer.wrap(new byte[BASE_SIZE + maskBytes.length]);
        buffer.putLong(epoch);
        id.write(buffer);
        buffer.put(maskBytes);
        content = buffer.array();
        try {
            s.update(content);
            signature = s.sign();
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign content", e);
        }
        hash = hashOf(content);
    }

    @Override
    public byte[] content() {
        return content;
    }

    public long getEpoch() {
        return getBuffer().getLong(EPOCH_INDEX);
    }

    public HashKey getId() {
        byte[] buf = new byte[32];
        ByteBuffer buffer = getBuffer();
        buffer.position(ID_INDEX);
        buffer.get(buf);
        return new HashKey(buf);
    }

    public BitSet getMask() {
        ByteBuffer buffer = getBuffer();
        buffer.position(MASK_INDEX);
        return BitSet.valueOf(buffer);
    }

    @Override
    public byte[] getSignature() {
        return signature;
    }

    @Override
    public byte[] hash() {
        return hash;
    }

    private ByteBuffer getBuffer() {
        return ByteBuffer.wrap(content);
    }
}
