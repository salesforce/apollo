/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies;

import java.nio.ByteBuffer;
import java.security.Signature;
import java.security.SignatureException;
import java.util.BitSet;
import java.util.UUID;

/**
 * Members issue notes to signal to other members that they are alive. In particular, a member will issue a new note as
 * a rebuttal to a false accusation. Notes are immutable after they are signed.
 * 
 * @author hal.hildebrand
 * @since 218
 */
public class Note implements Verifiable {
    private static final int EPOCH_INDEX = 0;
    private static final int ID_INDEX;
    private static final int MASK_INDEX;
    private static final int BASE_SIZE = 8 + 16;

    static {
        ID_INDEX = EPOCH_INDEX + 8;
        MASK_INDEX = ID_INDEX + 16;
    }

    /**
     * Binary content of the note.
     */
    private final byte[] content;

    /**
     * Signed with the member's private key.
     */
    private final byte[] signature;

    public Note(UUID id, long epoch, BitSet mask, Signature s) {
        byte[] maskBytes = mask.toByteArray();
        ByteBuffer buffer = ByteBuffer.wrap(new byte[BASE_SIZE + maskBytes.length]);
        buffer.putLong(epoch);
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        buffer.put(maskBytes);
        content = buffer.array();
        try {
            s.update(content);
            signature = s.sign();
        } catch (SignatureException e) {
            throw new IllegalStateException("Unable to sign content", e);
        }
    }

    public Note(byte[] content, byte[] signature) {
        this.content = content;
        this.signature = signature;
    }

    @Override
    public byte[] content() {
        return content;
    }

    public long getEpoch() {
        return getBuffer().getLong(EPOCH_INDEX);
    }

    public UUID getId() {
        return new UUID(getBuffer().getLong(ID_INDEX), getBuffer().getLong(ID_INDEX + 8));
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

    private ByteBuffer getBuffer() {
        return ByteBuffer.wrap(content);
    }
}
