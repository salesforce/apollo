/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies;

import java.nio.ByteBuffer;
import java.security.Signature;
import java.security.SignatureException;
import java.util.UUID;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class Accusation implements Verifiable {
    private static final int ACCUSED_INDEX;
    private static final int ACCUSER_INDEX;
    private static final int EPOCH_INDEX = 0;
    private static final int RING_INDEX;
    private static final int SIZE = 8 + 16 + 16 + 4;

    static {
        ACCUSER_INDEX = EPOCH_INDEX + 8;
        ACCUSED_INDEX = ACCUSER_INDEX + 16;
        RING_INDEX = ACCUSED_INDEX + 16;
    }

    private final byte[] content;
    private final byte[] signature;

    public Accusation(byte[] content, byte[] signature) {
        this.content = content;
        this.signature = signature;
    }

    public Accusation(long epoch, UUID accuser, int ringNumber, UUID accused, Signature s) {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[SIZE]);
        buffer.putLong(epoch);
        buffer.putLong(accuser.getMostSignificantBits());
        buffer.putLong(accuser.getLeastSignificantBits());
        buffer.putLong(accused.getMostSignificantBits());
        buffer.putLong(accused.getLeastSignificantBits());
        buffer.putInt(ringNumber);
        content = buffer.array();
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
    public UUID getAccused() {
        ByteBuffer buffer = ByteBuffer.wrap(content);
        return new UUID(buffer.getLong(ACCUSED_INDEX), buffer.getLong(ACCUSED_INDEX + 8));
    }

    /**
     * The identity of the accuser
     */
    public UUID getAccuser() {
        ByteBuffer buffer = ByteBuffer.wrap(content);
        return new UUID(buffer.getLong(ACCUSER_INDEX), buffer.getLong(ACCUSER_INDEX + 8));
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
}
