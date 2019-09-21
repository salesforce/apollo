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
import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.EntryType;
import com.salesforce.apollo.avro.Uuid;

/**
 * @author hal.hildebrand
 * @since 220
 */
public final class Conversion {
    public static final String SHA_256 = "sha-256";
    private final static Logger log = LoggerFactory.getLogger(Conversion.class);

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
    public static byte[] hashOf(Entry entry) {
        ByteBuffer buffer = entry.getData();
        buffer.mark();
        MessageDigest md;
        try {
            md = MessageDigest.getInstance(SHA_256);
        } catch (NoSuchAlgorithmException e1) {
            throw new IllegalStateException("Cannot get instance of message digest");
        }
        md.update((byte)entry.getType().ordinal());
        md.update(entry.getData().array());
        return md.digest();
    }

    public static DagEntry manifestDag(Entry entry) {
        SpecificDatumReader<DagEntry> reader = new SpecificDatumReader<>(DagEntry.class);
        byte[] data = entry.getData().array();
        BinaryDecoder decoder = CodecRecycler.decoder(data, 0, data.length);
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new IllegalStateException("unable to create bais for entry", e);
        } finally {
            CodecRecycler.release(decoder);
        }
    }

    public static Entry manifestEntry(byte[] data) {
        SpecificDatumReader<Entry> reader = new SpecificDatumReader<>(Entry.class);
        BinaryDecoder decoder = CodecRecycler.decoder(data, 0, data.length);
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new IllegalStateException("unable to create bais for entry", e);
        } finally {
            CodecRecycler.release(decoder);
        }
    }

    public static Entry serialize(DagEntry dag) {
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
        return new Entry(EntryType.DAG, ByteBuffer.wrap(out.toByteArray()));
    }

    public static byte[] serialize(Entry entry) {
        DatumWriter<Entry> writer = new SpecificDatumWriter<>(Entry.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = CodecRecycler.encoder(out, false);
        try {
            writer.write(entry, encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            log.debug("Error serializing DAG: {}", e);
        } finally {
            CodecRecycler.release(encoder);
        }
        return out.toByteArray();
    }

    public static UUID uuid(Uuid bits) {
        ByteBuffer buff = ByteBuffer.wrap(bits.bytes());
        UUID id = new UUID(buff.getLong(), buff.getLong());
        return id;
    }

    public static Uuid uuidBits(UUID id) {
        return new Uuid(bytes(id));
    }

    private Conversion() {
        // Hidden
    }

}
