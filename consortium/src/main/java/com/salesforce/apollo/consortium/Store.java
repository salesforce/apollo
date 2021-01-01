/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.consortium.CollaboratorContext.height;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.Certification;
import com.salesfoce.apollo.consortium.proto.Certifications;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesforce.apollo.consortium.support.CurrentBlock;
import com.salesforce.apollo.protocols.HashKey;

/**
 * Kind of a DAO for "nosql" block storage with MVStore from H2
 * 
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

    public List<Certification> certifications(long height) {
        byte[] bs = certifications.get(height);
        if (bs == null) {
            return null;
        }
        try {
            return Certifications.parseFrom(bs).getCertsList();
        } catch (InvalidProtocolBufferException e) {
            log.error("Could not deserialize certifications for {}", height);
            return Collections.emptyList();
        }
    }

    public CurrentBlock getBlock(long height) {
        byte[] block = block(height);
        try {
            return block == null ? null : new CurrentBlock(new HashKey(hash(height)), Block.parseFrom(block));
        } catch (InvalidProtocolBufferException e) {
            log.error("Cannot deserialize block height: {}", height, e);
            return null;
        }
    }

    public byte[] getBlockBits(Long height) {
        return blocks.get(height);
    }

    public CertifiedBlock getCertifiedBlock(long height) {
        return CertifiedBlock.newBuilder()
                             .setBlock(getBlock(height).getBlock())
                             .addAllCertifications(certifications(height))
                             .build();
    }

    public byte[] hash(long height) {
        return hashes.get(height);
    }

    public Map<Long, byte[]> hashes() {
        return hashes;
    }

    public void put(HashKey h, Block block) {
        long height = height(block);
        byte[] hash = h.bytes();
        blocks.put(height, block.toByteArray());
        hashes.put(height, hash);
        hashToHeight.put(hash, height);
    }

    public void put(HashKey hash, CertifiedBlock cb) {
        long height = height(cb.getBlock());
        Certifications certs = Certifications.newBuilder().addAllCerts(cb.getCertificationsList()).build();
        put(hash, cb.getBlock());
        certifications.put(height, certs.toByteArray());
    }
}
