/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.CollaboratorContext.height;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesforce.apollo.consortium.support.CurrentBlock;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Store {
    private static final String BLOCKS         = "BLOCKS";
    private static final String CERTIFICATIONS = "CERTIFICATIONS";
    private static final String HASH_TO_HEIGHT = "HASH_TO_HEIGHT";
    private static final String HASHES         = "HASHES";
    private static final Logger log            = LoggerFactory.getLogger(Store.class);

    private final MVMap<Long, byte[]> blocks;
    private final MVMap<Long, byte[]> certifications;
    private final MVMap<Long, byte[]> hashes;
    private final MVMap<byte[], Long> hashToHeight;

    public Store(MVStore store) {
        hashes = store.openMap(HASHES);
        blocks = store.openMap(BLOCKS);
        hashToHeight = store.openMap(HASH_TO_HEIGHT);
        certifications = store.openMap(CERTIFICATIONS);
    }

    public byte[] block(byte[] hash) {
        Long height = hashToHeight.get(hash);
        return height == null ? null : blocks.get(height);
    }

    public byte[] block(long height) {
        return blocks.get(height);
    }

    public Iterator<Long> blocksFrom(long from) {
        return blocks.keyIterator(from);
    }

    public CurrentBlock getBlock(long height) {
        byte[] block = block(height);
        try {
            return block == null ? null : new CurrentBlock(new HashKey(hash(height)), Block.parseFrom(block));
        } catch (InvalidProtocolBufferException e) {
            log.error("Cannot deserialize block height: {}", height);
            return null;
        }
    }

    public byte[] getBlockBits(Long height) {
        return blocks.get(height);
    }

    public byte[] hash(long height) {
        return hashes.get(height);
    }

    public Map<Long, byte[]> hashes() {
        return hashes;
    }

    public void put(HashKey hash, CertifiedBlock cb) {
        long height = height(cb.getBlock());
        put(hash, cb.getBlock());
        ByteArrayOutputStream certs = new ByteArrayOutputStream(cb.getCertificationsCount() * 1024);
        cb.getCertificationsList().forEach(cert -> {
            try {
                cert.toByteString().writeTo(certs);
            } catch (IOException e) {
                throw new IllegalStateException("unable to write certification for " + hash, e);
            }
        });
        certifications.put(height, certs.toByteArray());
    }

    public void put(HashKey h, Block block) {
        long height = height(block);
        byte[] hash = h.bytes();
        blocks.put(height, block.toByteArray());
        hashes.put(height, hash);
        hashToHeight.put(hash, height);
    }
}
