/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.h2.mvstore.MVMap;

import com.google.common.hash.BloomFilter;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.Slice;
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

    public List<Slice> fetchSegments(BloomFilter<Integer> bff, int maxSegments, BitsStreamGenerator entropy) {
        List<Integer> segments = IntStream.range(0, checkpoint.getSegmentsCount())
                                          .mapToObj(i -> i)
                                          .collect(new ReservoirSampler<Integer>(-1, maxSegments, entropy))
                                          .stream()
                                          .filter(s -> !bff.mightContain(s))
                                          .collect(Collectors.toList());
        List<Slice> slices = new ArrayList<>();
        for (int i : segments) {
            slices.add(Slice.newBuilder().setIndex(i).setBlock(ByteString.copyFrom(state.get(i))).build());
        }
        return slices;
    }
}
