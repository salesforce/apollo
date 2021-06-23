/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.security.SecureRandom;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.google.protobuf.Any;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.BloomFilter;
import com.salesforce.apollo.utils.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MemoryStore implements Store {

    private final DigestAlgorithm                     digestAlgorithm;
    private final ConcurrentNavigableMap<Digest, Any> immutable = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Digest, Any> mutable   = new ConcurrentSkipListMap<>();

    public MemoryStore(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

    @Override
    public void add(List<Any> entries) {
        entries.forEach(e -> {
            var key = digestAlgorithm.digest(e.toByteString());
            immutable.put(key, e);
        });
    }

    @Override
    public void bind(Digest key, Any value) {
        mutable.putIfAbsent(key, value);
    }

    @Override
    public Entries entriesIn(CombinedIntervals combined, int maxEntries) {
        Entries.Builder builder = Entries.newBuilder();
        combined.getIntervals().forEach(interval -> {
            immutable.keySet()
                     .subSet(interval.getBegin(), true, interval.getEnd(), false)
                     .stream()
                     .filter(e -> !interval.contains(e))
                     .limit(maxEntries)
                     .forEach(e -> builder.addRecords(immutable.get(e)));
        });
        return builder.build();
    }

    @Override
    public Any get(Digest key) {
        return immutable.get(key);
    }

    @Override
    public Any lookup(Digest key) {
        return mutable.get(key);
    }

    @Override
    public void populate(CombinedIntervals keyIntervals, double fpr, SecureRandom entropy) {
        keyIntervals.getIntervals().forEach(interval -> {
            NavigableSet<Digest> subSet = immutable.keySet()
                                                   .subSet(interval.getBegin(), true, interval.getEnd(), false);
            BloomFilter<Digest> bff = new DigestBloomFilter(entropy.nextInt(), subSet.size(), fpr);
            subSet.forEach(h -> bff.add(h));
            interval.setBff(bff);
        });
    }

    @Override
    public void purge(Digest key) {
        immutable.remove(key);
    }

    @Override
    public void put(Digest key, Any value) {
        immutable.putIfAbsent(key, value);
    }

    @Override
    public void remove(Digest key) {
        mutable.remove(key);
    }
}
