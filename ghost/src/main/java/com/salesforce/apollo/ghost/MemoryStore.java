/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import static com.salesforce.apollo.protocols.Conversion.hashOf;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import com.salesfoce.apollo.proto.DagEntry;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MemoryStore implements Store {

    private final ConcurrentNavigableMap<HashKey, DagEntry> data = new ConcurrentSkipListMap<>();

    @Override
    public void add(List<DagEntry> entries, List<HashKey> total) {
        entries.forEach(e -> {
            byte[] key = hashOf(e);
            data.put(new HashKey(key), e);
            total.add(new HashKey(key));
        });
    }

    @Override
    public List<DagEntry> entriesIn(CombinedIntervals combined, List<HashKey> have) { // TODO refactor using IBLT or
                                                                                      // such
        Set<HashKey> canHas = new ConcurrentSkipListSet<>();
        have.forEach(e -> canHas.add(e)); // really expensive
        List<DagEntry> entries = new ArrayList<>(); // TODO batching. we do need stinking batches for realistic scale
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
    public DagEntry get(HashKey key) {
        return data.get(key);
    }

    @Override
    public List<DagEntry> getUpdates(List<HashKey> want) {
        return want.stream().map(e -> data.get(e)).filter(e -> e != null).collect(Collectors.toList());
    }

    @Override
    public List<HashKey> have(CombinedIntervals keyIntervals) {
        return keyIntervals.getIntervals()
                           .stream()
                           .flatMap(i -> data.keySet().subSet(i.getBegin(), true, i.getEnd(), false).stream())
                           .collect(Collectors.toList());
    }

    @Override
    public List<HashKey> keySet() {
        return data.keySet().stream().collect(Collectors.toList());
    }

    @Override
    public void put(HashKey key, DagEntry value) {
        data.putIfAbsent(key, value);
    }
}
