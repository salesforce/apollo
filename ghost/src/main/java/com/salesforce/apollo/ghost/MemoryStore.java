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

import com.salesfoce.apollo.ghost.proto.Binding;
import com.salesfoce.apollo.ghost.proto.Content;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * A toy-ish implementation of the Ghost Store.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class MemoryStore implements Store {

    private final DigestAlgorithm                         digestAlgorithm;
    private final ConcurrentNavigableMap<Digest, Content> content = new ConcurrentSkipListMap<>();
    private final ConcurrentNavigableMap<Digest, Binding> mutable = new ConcurrentSkipListMap<>();

    public MemoryStore(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

    @Override
    public void add(List<Content> entries) {
        entries.forEach(e -> {
            var key = digestAlgorithm.digest(e.toByteString());
            content.put(key, e);
        });
    }

    @Override
    public void bind(Digest key, Binding value) {
        mutable.putIfAbsent(key, value);
    }

    @Override
    public Entries entriesIn(CombinedIntervals combined, int maxEntries) {
        Entries.Builder builder = Entries.newBuilder();
        combined.getIntervals().forEach(interval -> {
            content.keySet().subSet(interval.getBegin(), true, interval.getEnd(), false).stream()
                   .filter(e -> !interval.contentsContains(e)).limit(maxEntries)
                   .forEach(e -> builder.addContent(content.get(e)));
            mutable.keySet().subSet(interval.getBegin(), true, interval.getEnd(), false).stream()
                   .filter(e -> !interval.bindingsContains(e)).limit(maxEntries)
                   .forEach(e -> builder.addBinding(mutable.get(e)));
        });
        return builder.build();
    }

    @Override
    public Content get(Digest key) {
        return content.get(key);
    }

    @Override
    public Binding lookup(Digest key) {
        return mutable.get(key);
    }

    @Override
    public void populate(CombinedIntervals keyIntervals, double fpr, SecureRandom entropy) {
        keyIntervals.getIntervals().forEach(interval -> {
            NavigableSet<Digest> immutableSubset = content.keySet().subSet(interval.getBegin(), true, interval.getEnd(),
                                                                           false);
            BloomFilter<Digest> immutableBff = new DigestBloomFilter(entropy.nextLong(), immutableSubset.size(), fpr);
            immutableSubset.forEach(h -> immutableBff.add(h));
            interval.setContentsBff(immutableBff);

            NavigableSet<Digest> mutableSubset = mutable.keySet().subSet(interval.getBegin(), true, interval.getEnd(),
                                                                         false);
            BloomFilter<Digest> mutableBff = new DigestBloomFilter(entropy.nextLong(), mutableSubset.size(), fpr);
            mutableSubset.forEach(h -> mutableBff.add(h));
            interval.setBindingsBff(mutableBff);
        });
    }

    @Override
    public void purge(Digest key) {
        content.remove(key);
    }

    @Override
    public void put(Digest key, Content value) {
        content.putIfAbsent(key, value);
    }

    @Override
    public void remove(Digest key) {
        mutable.remove(key);
    }
}
