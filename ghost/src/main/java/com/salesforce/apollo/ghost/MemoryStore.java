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
import java.util.stream.Collectors;

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

    private final ConcurrentNavigableMap<Digest, Any> data = new ConcurrentSkipListMap<>();
    private final DigestAlgorithm                     digestAlgorithm;

    public MemoryStore(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

    @Override
    public void add(List<Any> entries) {
        entries.forEach(e -> {
            var key = digestAlgorithm.digest(e.toByteString());
            data.put(key, e);
        });
    }

    @Override
    public Entries entriesIn(CombinedIntervals combined, int maxEntries) {
        Entries.Builder builder = Entries.newBuilder();
        combined.getIntervals().forEach(interval -> {
            data.keySet()
                .subSet(interval.getBegin(), true, interval.getEnd(), false)
                .stream()
                .filter(e -> !interval.contains(e))
                .limit(maxEntries)
                .forEach(e -> builder.addRecords(data.get(e)));
        });
        return builder.build();
    }

    @Override
    public Any get(Digest key) {
        return data.get(key);
    }

    @Override
    public List<Any> getUpdates(List<Digest> want) {
        return want.stream().map(e -> data.get(e)).filter(e -> e != null).collect(Collectors.toList());
    }

    @Override
    public List<Digest> have(CombinedIntervals keyIntervals) {
        return keyIntervals.getIntervals()
                           .stream()
                           .flatMap(i -> data.keySet().subSet(i.getBegin(), true, i.getEnd(), false).stream())
                           .collect(Collectors.toList());
    }

    @Override
    public List<Digest> keySet() {
        return data.keySet().stream().collect(Collectors.toList());
    }

    @Override
    public void populate(CombinedIntervals keyIntervals, double fpr, SecureRandom entropy) {
        keyIntervals.getIntervals().forEach(interval -> {
            NavigableSet<Digest> subSet = data.keySet().subSet(interval.getBegin(), true, interval.getEnd(), false);
            BloomFilter<Digest> bff = new DigestBloomFilter(entropy.nextInt(), subSet.size() * 2, fpr);
            subSet.forEach(h -> bff.add(h));
            interval.setBff(bff);
        });
    }

    @Override
    public void put(Digest key, Any value) {
        data.putIfAbsent(key, value);
    }
}
