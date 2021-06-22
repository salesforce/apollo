/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import org.h2.mvstore.Cursor;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.BloomFilter;
import com.salesforce.apollo.utils.BloomFilter.DigestBloomFilter;
import com.salesforce.apollo.utils.DigestType;

/**
 * @author hal.hildebrand
 *
 */
public class GhostStore implements Store {
    private final Logger        log             = org.slf4j.LoggerFactory.getLogger(GhostStore.class);
    private final static String MV_MAP_TEMPLATE = "%s-GhostStore";

    private final MVMap<Digest, byte[]> data;
    private final DigestAlgorithm       digestAlgorithm;

    public GhostStore(Digest id, DigestAlgorithm digestAlgorithm, MVStore store) {
        this(digestAlgorithm, store.openMap(String.format(MV_MAP_TEMPLATE, id),
                                            new MVMap.Builder<Digest, byte[]>().keyType(new DigestType())));
    }

    public GhostStore(DigestAlgorithm digestAlgorithm, MVMap<Digest, byte[]> data) {
        this.data = data;
        this.digestAlgorithm = digestAlgorithm;
    }

    @Override
    public void add(List<Any> entries) {
        entries.forEach(e -> {
            var key = digestAlgorithm.digest(e.toByteString());
            data.put(key, e.toByteArray());
        });
    }

    @Override
    public Entries entriesIn(CombinedIntervals combined, int maxEntries) {
        Entries.Builder builder = Entries.newBuilder();
        for (KeyInterval interval : combined.getIntervals()) {
            Cursor<Digest, byte[]> cursor = new Cursor<Digest, byte[]>(data.getRootPage(), interval.getBegin(),
                    interval.getEnd());
            while (cursor.hasNext()) {
                Digest key = cursor.next();
                if (!interval.contains(key)) {
                    Any parsed;
                    try {
                        parsed = Any.parseFrom(data.get(key));
                        builder.addRecords(parsed);
                    } catch (InvalidProtocolBufferException e) {
                        log.debug("Unable to deserialize: {}", key);
                    }
                    if (builder.getRecordsCount() >= maxEntries) {
                        break;
                    }
                }
            }
        }
        return builder.build();
    }

    @Override
    public Any get(Digest key) {
        byte[] value = data.get(key);
        try {
            return value == null ? null : Any.parseFrom(value);
        } catch (InvalidProtocolBufferException e) {
            log.debug("Unable to deserialize: {}", key);
            throw new IllegalStateException("Unable to deserialize value for key: " + key);
        }
    }

    @Override
    public void populate(CombinedIntervals combined, double fpr, SecureRandom entropy) {
        combined.getIntervals().forEach(interval -> {
            List<Digest> subSet = new ArrayList<>();
            new Cursor<Digest, byte[]>(data.getRootPage(), interval.getBegin(),
                    interval.getEnd()).forEachRemaining(key -> subSet.add(key));
            BloomFilter<Digest> bff = new DigestBloomFilter(entropy.nextInt(), subSet.size(), fpr);
            subSet.forEach(h -> bff.add(h));
            interval.setBff(bff);
        });
    }

    @Override
    public void put(Digest key, Any value) {
        data.putIfAbsent(key, value.toByteArray());
    }

}
