/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.hash.BloomFilter;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesforce.apollo.membership.ReservoirSampler;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointState {
    public final Checkpoint        checkpoint;
    public final File              state;
    private final RandomAccessFile contents;

    public CheckpointState(Checkpoint checkpoint, File state) {
        this.checkpoint = checkpoint;
        this.state = state;
        try {
            contents = new RandomAccessFile(state, "r");
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Cannot access state contents: " + checkpoint.getCheckpoint() + " state: "
                    + state.getAbsolutePath());
        }
    }

    public void close() {
        try {
            contents.close();
        } catch (IOException e) {
        }
    }

    public List<byte[]> fetchSegments(BloomFilter<Integer> bff, int maxSegments, SecureRandom entropy) {
        List<Integer> segments = IntStream.range(0, checkpoint.getSegmentsCount())
                                          .mapToObj(i -> i)
                                          .collect(new ReservoirSampler<Integer>(-1, maxSegments, entropy))
                                          .stream()
                                          .filter(s -> !bff.mightContain(s))
                                          .collect(Collectors.toList());
        List<byte[]> slices = new ArrayList<>();
        Collections.sort(segments);
        byte[] buffer = new byte[checkpoint.getSegmentSize()];
        for (int i : segments) {
            try {
                contents.seek(i * checkpoint.getSegmentSize());
            } catch (IOException e) {
                throw new IllegalStateException("Error accessing state contents: " + checkpoint.getCheckpoint()
                        + " state: " + state.getAbsolutePath() + " @ " + i, e);
            }
            int read;
            try {
                read = contents.read(buffer);
            } catch (IOException e) {
                throw new IllegalStateException("Error accessing state contents: " + checkpoint.getCheckpoint()
                        + " state: " + state.getAbsolutePath() + " @ " + i, e);
            }
            slices.add(Arrays.copyOf(buffer, read));
        }
        return slices;
    }
}
