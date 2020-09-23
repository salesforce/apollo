/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.proto.DagEntry;

/**
 * @author hal.hildebrand
 * @since 220
 */
public final class Conversion {
    public static final String                      SHA_256        = "sha-256";
    private static final ThreadLocal<MessageDigest> MESSAGE_DIGEST = ThreadLocal.withInitial(() -> {
                                                                       try {
                                                                           return MessageDigest.getInstance(SHA_256);
                                                                       } catch (NoSuchAlgorithmException e) {
                                                                           throw new IllegalStateException(
                                                                                   "Unable to retrieve " + SHA_256
                                                                                           + " Message Digest instance",
                                                                                   e);
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
    public static byte[] hashOf(DagEntry entry) {
        return hashOf(serialize(entry));
    }

    public static DagEntry manifestDag(byte[] data) {
        try {
            return DagEntry.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("invalid data");
        }
    }

    public static byte[] serialize(DagEntry dag) {
        return dag.toByteArray();
    }

    private Conversion() {
        // Hidden
    }

}
