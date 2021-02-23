/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.h2.mvstore.MVMap;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.Slice;
import com.salesforce.apollo.protocols.BloomFilter;

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

    public void assemble(File file) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            for (byte[] bytes : IntStream.range(0, state.size())
                                         .mapToObj(hk -> state.get(hk))
                                         .collect(Collectors.toList())) {
                fos.write(bytes);
            }
            fos.flush();
        }
    }

    public List<Slice> fetchSegments(BloomFilter<Integer> bff, int maxSegments, BitsStreamGenerator entropy) {
        List<Slice> slices = new ArrayList<>();
        for (int i = 0; i < checkpoint.getSegmentsCount(); i++) {
            if (!bff.contains(i)) {
                slices.add(Slice.newBuilder().setIndex(i).setBlock(ByteString.copyFrom(state.get(i))).build());
                if (slices.size() >= maxSegments) {
                    break;
                }
            }
        }
        return slices;
    }
}
