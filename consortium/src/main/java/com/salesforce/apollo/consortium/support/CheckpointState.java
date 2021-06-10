/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.support;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.GZIPInputStream;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.h2.mvstore.MVMap;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.Slice;
import com.salesforce.apollo.utils.BloomFilter;
import com.salesforce.apollo.utils.Utils;

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

    public void assemble(File file) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file);
                GZIPInputStream gis = new GZIPInputStream(assembled())) {
            Utils.copy(gis, fos);
            gis.close();
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
