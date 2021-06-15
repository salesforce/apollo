/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import com.google.protobuf.Any;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MemoryStore implements Store {

    private final ConcurrentNavigableMap<Digest, Any> data = new ConcurrentSkipListMap<>();
    private final DigestAlgorithm digestAlgorithm;

    public MemoryStore(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

    @Override
    public void add(List<Any> entries, List<Digest> total) {
        entries.forEach(e -> {
            var key = digestAlgorithm.digest(e.toByteString());
            data.put(key, e);
            total.add(key);
        });
    }

    @Override
    public List<Any> entriesIn(CombinedIntervals combined, List<Digest> have) { // TODO refactor using IBLT or
                                                                                // such
        Set<Digest> canHas = new ConcurrentSkipListSet<>();
        have.forEach(e -> canHas.add(e)); // really expensive
        List<Any> entries = new ArrayList<>(); // TODO batching. we do need stinking batches for realistic scale
        combined.getIntervals().forEach(i -> {
            data.keySet()
                .subSet(i.getBegin(), true, i.getEnd(), false)
                .stream()
                .filter(e -> !canHas.contains(e))
                .forEach(e -> entries.add(data.get(e)));
        });
        return entries;
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
    public void put(Digest key, Any value) {
        data.putIfAbsent(key, value);
    }
}
