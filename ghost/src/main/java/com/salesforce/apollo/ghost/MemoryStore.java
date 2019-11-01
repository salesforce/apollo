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

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MemoryStore implements Store {

    private final ConcurrentNavigableMap<HashKey, DagEntry> data = new ConcurrentSkipListMap<>();

    @Override
    public void add(List<DagEntry> entries, List<HASH> total) {
        entries.forEach(e -> {
            byte[] key = hashOf(e);
            data.put(new HashKey(key), e);
            total.add(new HASH(key));
        });
    }

    @Override
    public List<DagEntry> entriesIn(CombinedIntervals combined, List<HASH> have) { // TODO refactor using IBLT or such
        Set<HashKey> canHas = new ConcurrentSkipListSet<>();
        have.stream().map(e -> new HashKey(e)).forEach(e -> canHas.add(e)); // really expensive
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
    public DagEntry get(HASH key) {
        return data.get(new HashKey(key));
    }

    @Override
    public List<DagEntry> getUpdates(List<HASH> want) {
        return want.stream()
                   .map(e -> new HashKey(e))
                   .map(e -> data.get(e))
                   .filter(e -> e != null)
                   .collect(Collectors.toList());
    }

    @Override
    public List<HASH> have(CombinedIntervals keyIntervals) {
        return keyIntervals.getIntervals()
                           .stream()
                           .flatMap(i -> data.keySet().subSet(i.getBegin(), true, i.getEnd(), false).stream())
                           .map(e -> e.toHash())
                           .collect(Collectors.toList());
    }

    @Override
    public List<HASH> keySet() {
        return data.keySet().stream().map(k -> k.toHash()).collect(Collectors.toList());
    }

    @Override
    public void put(HASH key, DagEntry value) {
        data.putIfAbsent(new HashKey(key), value);
    }
}
