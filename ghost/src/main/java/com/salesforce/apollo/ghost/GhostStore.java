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
import com.salesfoce.apollo.ghost.proto.Binding;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.BloomFilter;
import com.salesforce.apollo.utils.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class GhostStore implements Store {
    private final static String   IMMUTABLE_MAP_TEMPLATE = "%s.immutable.ghostStore";
    private final static String   MUTABLE_MAP_TEMPLATE   = "%s.mutable.ghostStore";
    private final DigestAlgorithm digestAlgorithm;

    private final MVMap<Digest, byte[]> immutable;
    private final Logger                log = org.slf4j.LoggerFactory.getLogger(GhostStore.class);
    private final MVMap<Digest, byte[]> mutable;

    public GhostStore(Digest id, DigestAlgorithm digestAlgorithm, MVStore store) {
        this(store.openMap(String.format(MUTABLE_MAP_TEMPLATE, id)), digestAlgorithm,
                store.openMap(String.format(IMMUTABLE_MAP_TEMPLATE, id)));
    }

    public GhostStore(MVMap<Digest, byte[]> mutable, DigestAlgorithm digestAlgorithm, MVMap<Digest, byte[]> immutable) {
        this.immutable = immutable;
        this.mutable = mutable;
        this.digestAlgorithm = digestAlgorithm;
    }

    @Override
    public void add(List<Any> entries) {
        entries.forEach(e -> {
            var key = digestAlgorithm.digest(e.toByteString());
            immutable.put(key, e.toByteArray());
        });
    }

    @Override
    public void bind(Digest key, Any value) {
        mutable.put(key, value.toByteArray());
    }

    @Override
    public Entries entriesIn(CombinedIntervals combined, int maxEntries) {
        Entries.Builder builder = Entries.newBuilder();
        for (KeyInterval interval : combined.getIntervals()) {
            immutableEntriesIn(maxEntries, builder, interval);
            mutableEntriesIn(maxEntries, builder, interval);
        }
        return builder.build();
    }

    @Override
    public Any get(Digest key) {
        byte[] value = immutable.get(key);
        try {
            return value == null ? null : Any.parseFrom(value);
        } catch (InvalidProtocolBufferException e) {
            log.debug("Unable to deserialize: {}", key);
            throw new IllegalStateException("Unable to deserialize immutable value for key: " + key);
        }
    }

    @Override
    public Any lookup(Digest key) {
        byte[] value = mutable.get(key);
        try {
            return value == null ? null : Any.parseFrom(value);
        } catch (InvalidProtocolBufferException e) {
            log.debug("Unable to deserialize: {}", key);
            throw new IllegalStateException("Unable to deserialize mutable value for key: " + key);
        }
    }

    @Override
    public void populate(CombinedIntervals combined, double fpr, SecureRandom entropy) {
        combined.getIntervals().forEach(interval -> {
            interval.setImmutableBff(populateImmutable(fpr, entropy, interval));
            interval.setMutableBff(populateMutable(fpr, entropy, interval));
        });
    }

    @Override
    public void purge(Digest key) {
        immutable.remove(key);
    }

    @Override
    public void put(Digest key, Any value) {
        if (immutable.get(key) == null) {
            immutable.putIfAbsent(key, value.toByteArray());
        }
    }

    @Override
    public void remove(Digest key) {
        mutable.remove(key);
    }

    private void immutableEntriesIn(int maxEntries, Entries.Builder builder, KeyInterval interval) {
        Cursor<Digest, byte[]> cursor = new Cursor<Digest, byte[]>(immutable.getRootPage(), interval.getBegin(),
                interval.getEnd());
        while (cursor.hasNext()) {
            Digest key = cursor.next();
            if (!interval.mutableContains(key)) {
                Any parsed;
                try {
                    parsed = Any.parseFrom(immutable.get(key));
                    builder.addImmutable(parsed);
                } catch (InvalidProtocolBufferException e) {
                    log.debug("Unable to deserialize immutable: {}", key);
                }
                if (builder.getImmutableCount() >= maxEntries) {
                    break;
                }
            }
        }
    }

    private void mutableEntriesIn(int maxEntries, Entries.Builder builder, KeyInterval interval) {
        Cursor<Digest, byte[]> cursor = new Cursor<Digest, byte[]>(mutable.getRootPage(), interval.getBegin(),
                interval.getEnd());
        while (cursor.hasNext()) {
            Digest key = cursor.next();
            if (!interval.mutableContains(key)) {
                Any parsed;
                try {
                    parsed = Any.parseFrom(mutable.get(key));
                    builder.addMutable(Binding.newBuilder().setKey(key.toByteString()).setValue(parsed));
                } catch (InvalidProtocolBufferException e) {
                    log.debug("Unable to deserialize mutable: {}", key);
                }
                if (builder.getImmutableCount() >= maxEntries) {
                    break;
                }
            }
        }
    }

    private BloomFilter<Digest> populateImmutable(double fpr, SecureRandom entropy, KeyInterval interval) {
        List<Digest> subSet = new ArrayList<>();
        new Cursor<Digest, byte[]>(immutable.getRootPage(), interval.getBegin(),
                interval.getEnd()).forEachRemaining(key -> subSet.add(key));
        BloomFilter<Digest> bff = new DigestBloomFilter(entropy.nextInt(), subSet.size(), fpr);
        subSet.forEach(h -> bff.add(h));
        return bff;
    }

    private BloomFilter<Digest> populateMutable(double fpr, SecureRandom entropy, KeyInterval interval) {
        List<Digest> subSet = new ArrayList<>();
        new Cursor<Digest, byte[]>(mutable.getRootPage(), interval.getBegin(),
                interval.getEnd()).forEachRemaining(key -> subSet.add(key));
        BloomFilter<Digest> bff = new DigestBloomFilter(entropy.nextInt(), subSet.size(), fpr);
        subSet.forEach(h -> bff.add(h));
        return bff;
    }

}
