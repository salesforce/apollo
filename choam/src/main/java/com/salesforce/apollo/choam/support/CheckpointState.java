/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.choam.proto.Checkpoint;
import com.salesfoce.apollo.choam.proto.Slice;
import com.salesforce.apollo.bloomFilters.BloomFilter;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.HexBloom;
import com.salesforce.apollo.utils.Utils;
import org.h2.mvstore.MVMap;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

/**
 * @author hal.hildebrand
 */
public class CheckpointState {
    public final Checkpoint             checkpoint;
    public final MVMap<Integer, byte[]> state;

    public CheckpointState(Checkpoint checkpoint, MVMap<Integer, byte[]> stored) {
        this.checkpoint = checkpoint;
        this.state = stored;
    }

    public void assemble(File file) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file);
             GZIPInputStream gis = new GZIPInputStream(assembled())) {
            Utils.copy(gis, fos);
        }
    }

    public InputStream assembled() {
        return new SequenceInputStream(new Enumeration<InputStream>() {
            int current = 0;

            @Override
            public boolean hasMoreElements() {
                return current < state.size();
            }

            @Override
            public InputStream nextElement() {
                if (current >= state.size()) {
                    throw new NoSuchElementException();
                }
                int c = current;
                current++;
                byte[] buf = state.get(c);
                return new ByteArrayInputStream(buf);
            }
        });
    }

    public void close() {
        state.clear();
    }

    public List<Slice> fetchSegments(BloomFilter<Integer> bff, int maxSegments) {
        List<Slice> slices = new ArrayList<>();
        for (int i = 0; i < checkpoint.getCount(); i++) {
            if (!bff.contains(i)) {
                slices.add(Slice.newBuilder().setIndex(i).setBlock(ByteString.copyFrom(state.get(i))).build());
                if (slices.size() >= maxSegments) {
                    break;
                }
            }
        }
        return slices;
    }

    public boolean validate(HexBloom diadem, Digest initial) {
        var crowns = diadem.crowns();
        var algorithm = crowns.get(0).getAlgorithm();
        var accumulator = new HexBloom.Accumulator(diadem.getCardinality(), crowns.size(), initial);
        state.keyIterator(0).forEachRemaining(i -> {
            byte[] buf = state.get(i);
            accumulator.add(algorithm.digest(buf));
        });
        var candidates = accumulator.wrappedCrowns();
        for (int i = 0; i < crowns.size(); i++) {
            if (!crowns.get(i).equals(candidates.get(i))) {
                LoggerFactory.getLogger(CheckpointState.class)
                             .warn("Crown[{}] expected: {} found: {}", i, crowns.get(i), candidates.get(i));
                return false;
            }
        }
        return true;
    }
}
