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
import com.salesforce.apollo.utils.Utils;
import org.h2.mvstore.MVMap;

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
