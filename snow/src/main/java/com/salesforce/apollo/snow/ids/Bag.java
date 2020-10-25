/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.ids;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * @author hal.hildebrand
 *
 */
public class Bag {
    public static class Mode {
        public final int freq;
        public final ID  id;

        public Mode(ID id, int freq) {
            this.id = id;
            this.freq = freq;
        }
    }

    private final Map<ID, Integer> counts       = new HashMap<>();
    private final Set<ID>          metThreshold = new HashSet<>();
    private ID                     mode;
    private int                    modeFreq;
    private int                    size;
    private int                    threshold;

    public void add(ID... ids) {
        for (ID id : ids) {
            addCount(id, 1);
        }
    }

    public void addCount(ID id, int count) {
        if (count <= 0) {
            return;
        }
        int totalCount = counts.getOrDefault(id, 0) + count;
        counts.put(id, totalCount);
        size += count;
        if (totalCount > modeFreq) {
            mode = id;
            modeFreq = totalCount;
        }
        if (totalCount >= threshold) {
            metThreshold.add(id);
        }
    }

    public int count(ID id) {
        return counts.getOrDefault(id, 0);
    }

    public boolean eq(Bag b) {
        if (size != b.size) {
            return false;
        }
        for (Entry<ID, Integer> e : counts.entrySet()) {
            if (e.getValue() != b.count(e.getKey())) {
                return false;
            }
        }
        return true;
    }

    public Bag filter(int start, int end, ID id) {
        Bag filtered = new Bag();
        counts.forEach((vote, count) -> {
            if (ID.equalSubset(start, end, vote, id)) {
                filtered.addCount(vote, count);
            }
        });
        return filtered;
    }

    public Mode mode() {
        return new Mode(mode, modeFreq);
    }

    public void setThreshold(int threshold) {
        if (this.threshold == threshold) {
            return;
        }
        this.threshold = threshold;
        metThreshold.clear();

        counts.forEach((vote, count) -> {
            if (count >= threshold) {
                metThreshold.add(vote);
            }
        });
    }

    public int size() {
        return size;
    }

    public Bag[] split(int index) {
        Bag[] splitVotes = new Bag[] { new Bag(), new Bag() };
        counts.forEach((vote, count) -> {
            splitVotes[vote.bit(index)].addCount(vote, count);
        });
        return splitVotes;
    }

    public Set<ID> threshold() {
        return metThreshold;
    }
}
