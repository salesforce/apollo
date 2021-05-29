/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.UUID;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.crypto.BbBackedInputStream;

/**
 * @author hal.hildebrand
 * @since 220
 */
public final class Conversion {
    public static final String DEFAULT_SIGNATURE_ALGORITHM = "SHA256withRSA";
    public static final String SHA_256                     = "sha-256";

    private static final ThreadLocal<MessageDigest> MESSAGE_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance(SHA_256);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Unable to retrieve " + SHA_256 + " Message Digest instance", e);
        }
    });

    public static byte[] bytes(UUID itself) {
        ByteBuffer buff = ByteBuffer.wrap(new byte[16]);
        buff.putLong(itself.getMostSignificantBits());
        buff.putLong(itself.getLeastSignificantBits());
        return buff.array();
    }

    /**
     * @param entry
     * @return the hash value of the entry
     */
    public static byte[] hashOf(byte[]... bytes) {
        MessageDigest md = MESSAGE_DIGEST.get();
        md.reset();
        for (byte[] entry : bytes) {
            md.update(entry);
        }
        return md.digest();
    }

    /**
     * @param entry
     * @return the hash value of the entry
     */
    public static byte[] hashOf(byte[] bytes, int len) {
        MessageDigest md = MESSAGE_DIGEST.get();
        md.reset();
        md.update(bytes, 0, len);
        return md.digest();
    }

    public static byte[] hashOf(ByteBuffer... buffers) {
        InputStream is = BbBackedInputStream.aggregate(buffers);
        return hashOf(is);
    }

    public static byte[] hashOf(ByteString... byteString) {
        InputStream is = BbBackedInputStream.aggregate(byteString);
        return hashOf(is);
    }

    /**
     * @param entry
     * @return the hash value of the entry
     */
    public static byte[] hashOf(DagEntry entry) {
        return hashOf(entry.toByteString());
    }

    public static byte[] hashOf(InputStream is) {
        MessageDigest md = MESSAGE_DIGEST.get();
        md.reset();
        byte[] buf = new byte[md.getDigestLength()];
        try {
            for (int read = is.read(buf); read >= 0; read = is.read(buf)) {
                md.update(buf, 0, read);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error reading from buffers, cannot generate hash", e);
        }
        return md.digest();
    }

    public static byte[] hashOf(List<ByteBuffer> buffers) {
        InputStream is = BbBackedInputStream.aggregate(buffers);
        return hashOf(is);
    }

    public static DagEntry manifestDag(byte[] data) {
        if (data.length == 0) {
            System.out.println(" Invalid data");
        }
        try {
            DagEntry entry = DagEntry.parseFrom(data);
            return entry;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid data");
        }
    }

    public static DagEntry manifestDag(ByteString data) {
        if (data.size() == 0) {
            System.out.println(" Invalid data");
        }
        try {
            DagEntry entry = DagEntry.parseFrom(data);
            return entry;
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid data");
        }
    }

    public static byte[] serialize(DagEntry dag) {
        byte[] bytes = dag.toByteArray();
        assert bytes.length > 0 : " Invalid serialization: " + dag.getDescription();
        return bytes;
    }

    private Conversion() {
        // Hidden
    }

}
