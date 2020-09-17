/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.avro.DagEntry;

/**
 * @author hal.hildebrand
 * @since 220
 */
public final class Conversion {
    public static final String                      SHA_256        = "sha-256";
    private final static Logger                     log            = LoggerFactory.getLogger(Conversion.class);
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
        SpecificDatumReader<DagEntry> reader = new SpecificDatumReader<>(DagEntry.class);
        BinaryDecoder decoder = CodecRecycler.decoder(data, 0, data.length);
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new IllegalStateException("unable to create bais for entry", e);
        } finally {
            CodecRecycler.release(decoder);
        }
    }

    public static byte[] serialize(DagEntry dag) {
        DatumWriter<DagEntry> writer = new SpecificDatumWriter<>(DagEntry.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = CodecRecycler.encoder(out, false);
        try {
            writer.write(dag, encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            log.debug("Error serializing DAG: {}", e);
        } finally {
            CodecRecycler.release(encoder);
        }
        return out.toByteArray();
    }

    private Conversion() {
        // Hidden
    }

}
