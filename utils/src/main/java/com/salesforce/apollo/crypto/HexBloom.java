/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.salesfoce.apollo.utils.proto.HexBloome;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

/**
 * Based on the paper <a href="https://eprint.iacr.org/2021/773.pdf">HEX-BLOOM:
 * An Efficient Method for Authenticity and Integrity Verification in
 * Privacy-preserving Computing</a>
 *
 * @author hal.hildebrand
 *
 */
public class HexBloom {

    /**
     * Construct a HexBloom.
     *
     * @param currentMembership - list of digests that correspond to the supplied
     *                          crowns
     * @param added             - digests added that are not present in the
     *                          currentMembership list
     * @param crowns            - the current crown state corresponding to the
     *                          currentMembership
     * @param removed           - digests removed that are present in the
     *                          currentMembership list
     * @param hashes            - the list of functions for computing the hash of a
     *                          digest for a given crown
     * @param fpr               - desired false positive rate for membership
     *                          bloomfilter
     * @return the HexBloom representing the new state
     */
    public static HexBloom construct(List<Digest> currentMembership, List<Digest> added, List<Digest> crowns,
                                     List<Digest> removed, List<Function<Digest, Digest>> hashes, double fpr) {
        assert crowns.size() == hashes.size();
        var cardinality = currentMembership.size() + added.size() - removed.size();
        var membership = new BloomFilter.DigestBloomFilter(Entropy.nextSecureLong(), cardinality, fpr);
        var crwns = crowns.stream().map(d -> new AtomicReference<>(d)).toList();
        added.forEach(d -> {
            for (int i = 0; i < crwns.size(); i++) {
                crwns.get(i).accumulateAndGet(hashes.get(i).apply(d), (a, b) -> a.xor(b));
            }
            membership.add(d);
        });
        removed.forEach(d -> {
            for (int i = 0; i < crwns.size(); i++) {
                crwns.get(i).accumulateAndGet(hashes.get(i).apply(d), (a, b) -> a.xor(b));
            }
        });
        currentMembership.forEach(d -> membership.add(d));
        return new HexBloom(cardinality, crwns.stream().map(ad -> ad.get()).toList(), membership);
    }

    public static HexBloom from(HexBloome hb) {
        return new HexBloom(hb);
    }

    private final int           cardinality = 0;
    private final Digest[]      crowns;
    private BloomFilter<Digest> membership;

    public HexBloom(HexBloome hb) {
        this(hb.getCardinality(), hb.getCrownsList().stream().map(d -> Digest.from(d)).toList(),
             BloomFilter.from(hb.getMembership()));
    }

    public HexBloom(int cardinality, List<Digest> crowns, BloomFilter<Digest> membership) {
        this.crowns = new Digest[crowns.size()];
        for (int i = 0; i < crowns.size(); i++) {
            this.crowns[i] = crowns.get(i);
        }
        this.membership = membership;
    }

    public boolean contains(Digest digest) {
        return membership.contains(digest);
    }

    public List<Digest> crowns() {
        return Arrays.asList(crowns);
    }

    public boolean equivalent(HexBloom other) {
        if (cardinality != other.cardinality) {
            return false;
        }
        if (crowns.length != other.crowns.length) {
            return false;
        }
        for (int i = 0; i < crowns.length; i++) {
            if (!crowns[i].equals(other.crowns[i])) {
                return false;
            }
        }
        return membership.equivalent(other.membership);
    }

    public int getCardinality() {
        return cardinality;
    }

    public HexBloome toHexBloome() {
        return HexBloome.newBuilder()
                        .setCardinality(cardinality)
                        .addAllCrowns(Arrays.asList(crowns).stream().map(d -> d.toDigeste()).toList())
                        .setMembership(membership.toBff())
                        .build();
    }
}
