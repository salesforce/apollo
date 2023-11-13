/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.bloomFilters;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.BitSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import com.salesforce.apollo.bloomFilters.Hash;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ConcurrentHashMultiset;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.bloomFilters.Hash.BytesHasher;
import com.salesforce.apollo.bloomFilters.Hash.IntHasher;

/**
 * @author hal.hildebrand
 *
 */
public class HashTest {

    @Test
    public void avalanche() {
        IntStream.of(new int[] { 2, 4, 8, 16, 32 }).parallel().forEach(i -> {
            avalanche(i * 8);
        });
    }

    private void avalanche(Integer size) {
        ConcurrentHashMultiset<Integer> distribution = ConcurrentHashMultiset.create();
        ConcurrentHashMultiset<Integer> combined = ConcurrentHashMultiset.create();
        long seed = Entropy.nextSecureLong();
        BytesHasher hash = new Hash.BytesHasher();
        IntStream.range(0, 10_000_000).parallel().forEach(i -> {
            byte[] a = new byte[size / 8];
            Entropy.nextSecureBytes(a);
            BitSet vector = BitSet.valueOf(a);
            hash.process(vector.toByteArray(), seed);
            long aH1 = hash.h1;
            long aH2 = hash.h2;

            vector.flip(Entropy.nextSecureInt(size));

            hash.process(vector.toByteArray(), seed);
            long bH1 = hash.h1;
            long bH2 = hash.h2;

            distribution.add(Long.bitCount(aH1 ^ bH1));
            distribution.add(Long.bitCount(aH2 ^ bH2));
            combined.add(Long.bitCount((aH1 + aH2) ^ (bH1 + bH2)));
        });
        record Maxi(int bit, int count) {}
        AtomicReference<Maxi> distMax = new AtomicReference<>(new Maxi(-1, -1));
        AtomicReference<Maxi> combMax = new AtomicReference<>(new Maxi(-1, -1));

        distribution.forEachEntry((bit, count) -> {
            if (distMax.get().count < count) {
                distMax.set(new Maxi(bit, count));
            }
        });
        combined.forEachEntry((bit, count) -> {
            if (combMax.get().count < count) {
                combMax.set(new Maxi(bit, count));
            }
        });
        assertEquals(32, distMax.get().bit, "size: " + size + " distibution: " + distribution.toString());
        assertEquals(32, combMax.get().bit, "size: " + size + "combined: " + combined.toString());
    }

    @Test
    void kIntHashes() {
        IntStream.range(3, 7).parallel().forEach(k -> {
            IntStream.of(new int[] { 80, 1500, 10_000, 100_000 }).parallel().forEach(m -> {
                intHashes(k, m);
            });
        });
    }

    private void intHashes(int k, int m) {
        ConcurrentHashMultiset<Integer> frequency = ConcurrentHashMultiset.create();
        long seed = 0x1638;

        IntStream.range(0, m * 2).parallel().forEach(i -> {
            IntHasher hash = new Hash.IntHasher();
            int[] hashes = hash.hashes(k, Entropy.nextSecureInt(), m, seed);
            IntStream.of(hashes).forEach(j -> frequency.add(j));
        });
        double missing = m - frequency.elementSet().size();
        double pc = ((missing / m) * 100.0);
        if (pc < 0.5 | missing < 2.0) {
            System.out.println(String.format("Missing: k: %s m: %s missing: %s : %s seed: %s", k, m, (int) missing, pc,
                                             seed));
        } else {
            System.out.println(String.format("OK: k: %s m: %s missing: %s : %s", k, m, (int) missing, pc));
        }
    }
}
