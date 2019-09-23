/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import com.salesforce.apollo.avalanche.Dag.DagInsert;
import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hhildebrand
 */
public class InsertQ {
    private final Map<HashKey, DagInsert> contents = new ConcurrentSkipListMap<HashKey, Dag.DagInsert>();
    private final BlockingDeque<HashKey> deque = new LinkedBlockingDeque<>();

    public void add(HASH hashKey, DagEntry dagEntry, Entry entry, HASH conflictSet, boolean noOp, int targetRound) {
        HashKey key = new HashKey(hashKey);
        contents.computeIfAbsent(key, k -> {
            deque.add(k);
            return new DagInsert(hashKey, dagEntry, entry, conflictSet, noOp, targetRound);
        });
    }

    public List<DagInsert> next(int batch) {
        List<DagInsert> next = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
            HashKey key;
            try {
                key = deque.poll(0, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                break;
            }
            if (key != null) {
                DagInsert removed = contents.remove(key);
                if (removed != null) {
                    next.add(removed);
                }
            } else {
                break;
            }
        }
        return next;
    }

    public int size() {
        return deque.size();
    }
}
