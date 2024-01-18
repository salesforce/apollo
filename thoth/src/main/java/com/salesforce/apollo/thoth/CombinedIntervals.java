/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.thoth.proto.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author hal.hildebrand
 */
public class CombinedIntervals implements Predicate<Digest> {
    private final List<KeyInterval> intervals = new ArrayList<>();

    public CombinedIntervals(KeyInterval... allIntervals) {
        this(Arrays.asList(allIntervals));
    }

    public CombinedIntervals(List<KeyInterval> allIntervals) {
        if (allIntervals.isEmpty()) {
            return;
        }
        // if both intervals begin the same
        // compare their ends
        allIntervals.sort(Comparator.comparing(KeyInterval::getBegin).thenComparing(KeyInterval::getEnd));
        KeyInterval current = allIntervals.getFirst();
        intervals.add(current);
        for (int i = 1; i < allIntervals.size(); i++) {
            KeyInterval next = allIntervals.get(i);

            int compare = current.getEnd().compareTo(next.getBegin());
            if (compare < 0) {
                intervals.add(next);
                current = next;
            } else {
                // overlapping intervals
                current = new KeyInterval(current.getBegin(), next.getEnd());
                intervals.set(intervals.size() - 1, current);
            }
        }
    }

    public Stream<KeyInterval> intervals() {
        return intervals.stream();
    }

    @Override
    public boolean test(Digest t) {
        return intervals.stream().anyMatch(i -> i.test(t));
    }

    public List<Interval> toIntervals() {
        return intervals.stream()
                        .map(e -> Interval.newBuilder()
                                          .setStart(e.getBegin().toDigeste())
                                          .setEnd(e.getEnd().toDigeste())
                                          .build())
                        .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "CombinedIntervals [intervals=" + intervals + "]";
    }
}
