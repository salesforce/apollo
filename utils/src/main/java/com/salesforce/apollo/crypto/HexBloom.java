/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.crypto;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    private static final double                   DEFAULT_FPR = 0.001;
    private static final Function<Digest, Digest> IDENTITY    = d -> d;

    /**
     * Construct a HexBloom with a membership bloomfilter using the default false
     * positive rate and the default hash transforms for each crown
     *
     * @param currentMembership - list of digests that correspond to the supplied
     *                          crowns
     * @param added             - digests added that are not present in the
     *                          currentMembership list
     * @param crowns            - the current crown state corresponding to the
     *                          currentMembership
     * @param removed           - digests removed that are present in the
     *                          currentMembership list
     * @return the HexBloom representing the new state
     */
    public static HexBloom construct(List<Digest> currentMembership, List<Digest> added, List<Digest> crowns,
                                     List<Digest> removed) {
        return construct(currentMembership, added, crowns, removed, hashes(crowns.size()), DEFAULT_FPR);
    }

    /**
     * Construct a HexBloom with a membership bloomfilter using the default false
     * positive rate
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
     * @return the HexBloom representing the new state
     */
    public static HexBloom construct(List<Digest> currentMembership, List<Digest> added, List<Digest> crowns,
                                     List<Digest> removed, List<Function<Digest, Digest>> hashes) {
        return construct(currentMembership, added, crowns, removed, hashes, DEFAULT_FPR);
    }

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
        if (hashes.size() != crowns.size()) {
            throw new IllegalArgumentException("Size of supplied hash functions: " + hashes.size()
            + " must equal the # of crowns: " + crowns.size());
        }
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

    /**
     * Answer the default hash for a crown positiion
     *
     * @param index
     * @return the hash transform
     */
    public static Function<Digest, Digest> hash(int index) {
        return index == 0 ? IDENTITY : d -> d.prefix(index);
    }

    /**
     * Answer the default hash transforms for the number of crowns
     *
     * @param crowns
     * @return
     */
    public static List<Function<Digest, Digest>> hashes(int crowns) {
        return IntStream.range(0, crowns).mapToObj(i -> hash(i)).toList();
    }

    /**
     * Answer the default wrapping hash for a crown positiion
     *
     * @param index
     * @return the wrapping hash transform
     */
    public static Function<Digest, Digest> hashWrap(int index) {
        return d -> d.prefix(index);
    }

    /**
     * Answer the default wrapping hash transforms for the number of crowns
     *
     * @param crowns
     * @return
     */
    public static List<Function<Digest, Digest>> hashWraps(int crowns) {
        return IntStream.range(0, crowns).mapToObj(i -> hashWrap(i)).toList();
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
        return toHexBloome(hashWraps(crowns.length));
    }

    /**
     * Answer the serialized receiver, with crowns hashed using the supplied hashes
     * functions
     *
     * @param hashes
     * @return
     */
    public HexBloome toHexBloome(List<Function<Digest, Digest>> hashes) {
        if (hashes.size() != crowns.length) {
            throw new IllegalArgumentException("Size of supplied hash functions: " + hashes.size()
            + " must equal the # of crowns: " + crowns.length);
        }
        final var builder = HexBloome.newBuilder().setCardinality(cardinality).setMembership(membership.toBff());
        for (int i = 0; i < crowns.length; i++) {
            builder.addCrowns(hashes.get(i).apply(crowns[i]).toDigeste());
        }
        return builder.build();
    }

    /**
     * Answer the serialized receiver, using no transformation on the crowns
     *
     * @return
     */
    public HexBloome toIdentityHexBloome() {
        return toHexBloome(IntStream.range(0, crowns.length).mapToObj(i -> IDENTITY).toList());
    }

    /**
     * Validate that the supplied members matches the receiver's crowns. All members
     * must be included in the membership bloomfilter, all calculated crowns must
     * match and the cardinality must match.
     *
     * @param members - list of member digests
     * @return true if validated
     */
    public boolean validate(List<Digest> members) {
        return validate(members.stream(), hashes(crowns.length));
    }

    /**
     * Validate that the supplied members matches the receiver's crowns. All members
     * must be included in the membership bloomfilter, all calculated crowns must
     * match and the cardinality must match.
     *
     * @param members - lsit of member digests
     * @param hashes  - hash functions for computing crowns
     * @return true if validated
     */
    public boolean validate(List<Digest> members, List<Function<Digest, Digest>> hashes) {
        return validate(members.stream(), hashes);
    }

    /**
     * Validate that the supplied members matches the receiver's crowns. All members
     * must be included in the membership bloomfilter, all calculated crowns must
     * match and the cardinality must match.
     *
     * @param members - stream of member digests
     * @return true if validated
     */
    public boolean validate(Stream<Digest> members) {
        return validate(members, hashes(crowns.length));
    }

    /**
     * Validate that the supplied members matches the receiver's crowns. All members
     * must be included in the membership bloomfilter, all calculated crowns must
     * match and the cardinality must match.
     *
     * @param members - stream of member digests
     * @param hashes  - hash functions for computing crowns
     * @return true if validated
     */
    public boolean validate(Stream<Digest> members, List<Function<Digest, Digest>> hashes) {
        if (hashes.size() != crowns.length) {
            throw new IllegalArgumentException("Size of supplied hash functions: " + hashes.size()
            + " must equal the # of crowns: " + crowns.length);
        }
        var count = new AtomicInteger();
        @SuppressWarnings("unchecked")
        AtomicReference<Digest>[] calculated = (AtomicReference<Digest>[]) IntStream.range(0, crowns.length)
                                                                                    .mapToObj(i -> new AtomicReference<Digest>())
                                                                                    .toArray();
        members.forEach(d -> {
            for (int i = 0; i < crowns.length; i++) {
                calculated[i].accumulateAndGet(hashes.get(i).apply(d), (a, b) -> a.xor(b));
            }
            count.incrementAndGet();
        });
        if (count.get() != cardinality) {
            return false;
        }
        for (int i = 0; i < crowns.length; i++) {
            if (!calculated[i].get().equals(crowns[i])) {
                return false;
            }
        }
        return true;
    }
}
