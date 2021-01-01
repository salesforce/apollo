/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVMap;

import com.google.common.hash.BloomFilter;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesforce.apollo.membership.ReservoirSampler;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointState {
    public final Checkpoint             checkpoint;
    public final MVMap<Integer, byte[]> state;

    public CheckpointState(Checkpoint checkpoint, MVMap<Integer, byte[]> stored) {
        this.checkpoint = checkpoint;
        this.state = stored;
    }

    public void close() {
        state.clear();
    }

    public List<byte[]> fetchSegments(BloomFilter<Integer> bff, int maxSegments, SecureRandom entropy) {
        List<Integer> segments = IntStream.range(0, checkpoint.getSegmentsCount())
                                          .mapToObj(i -> i)
                                          .collect(new ReservoirSampler<Integer>(-1, maxSegments, entropy))
                                          .stream()
                                          .filter(s -> !bff.mightContain(s))
                                          .collect(Collectors.toList());
        List<byte[]> slices = new ArrayList<>();
        for (int i : segments) {
            slices.add(state.get(i));
        }
        return slices;
    }
}
