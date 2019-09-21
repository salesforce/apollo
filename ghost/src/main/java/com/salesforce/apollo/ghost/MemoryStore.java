/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.GhostUpdate;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class MemoryStore implements Store {
    private final ConcurrentNavigableMap<HashKey, Entry> data = new ConcurrentSkipListMap<>();

    @Override
    public Entry get(HASH key) {
        return data.get(new HashKey(key));
    }

    @Override
    public List<HASH> keySet() {
        return data.keySet().stream().map(k -> k.toHash()).collect(Collectors.toList());
    }

    @Override
    public void put(HASH key, Entry value) {
        data.putIfAbsent(new HashKey(key), value);
    }

    @Override
    public GhostUpdate updatesFor(CombinedIntervals theirIntervals, List<HashKey> digests,
            CombinedIntervals myIntervals) {
        NavigableSet<HashKey> digestSet = new TreeSet<>(digests);
        List<HASH> want = new ArrayList<>();
        myIntervals.getIntervals().forEach(i -> {
            Sets.difference(digestSet.subSet(i.getBegin(), true, i.getEnd(), false),
                            data.keySet().subSet(i.getBegin(), true, i.getEnd(), false))
                .forEach(e -> want.add(e.toHash()));
        });
        List<Entry> updates = new ArrayList<Entry>();
        theirIntervals.getIntervals().forEach(i -> {
            Sets.difference(data.keySet().subSet(i.getBegin(), true, i.getEnd(), false),
                            digestSet.subSet(i.getBegin(), true, i.getEnd(), false))
                .forEach(e -> updates.add(data.get(e)));
        });
        return new GhostUpdate(myIntervals.toIntervals(), want, updates);
    }
}
