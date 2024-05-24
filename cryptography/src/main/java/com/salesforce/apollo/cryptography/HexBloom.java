/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.cryptography;

import com.salesforce.apollo.bloomFilters.BloomFilter;
import com.salesforce.apollo.bloomFilters.Primes;
import com.salesforce.apollo.cryptography.proto.HexBloome;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Based on the paper <a href="https://eprint.iacr.org/2021/773.pdf">HEX-BLOOM: An Efficient Method for Authenticity and
 * Integrity Verification in Privacy-preserving Computing</a>
 *
 * @author hal.hildebrand
 */
public class HexBloom {

    public static final  double                   DEFAULT_FPR      = 0.0001;
    public static final  long                     DEFAULT_SEED     = Primes.PRIMES[666];
    private static final Function<Digest, Digest> IDENTITY         = d -> d;
    private static final int                      MINIMUM_BFF_CARD = 100;

    private final int                 cardinality;
    private final Digest[]            crowns;
    private final BloomFilter<Digest> membership;

    public HexBloom(Digest initial, int count) {
        assert count > 0;
        var hashes = hashes(count);
        crowns = new Digest[count];
        cardinality = 0;
        membership = new BloomFilter.DigestBloomFilter(0x666, MINIMUM_BFF_CARD, DEFAULT_FPR);
        for (int i = 0; i < crowns.length; i++) {
            crowns[i] = hashes.get(i).apply(initial);
        }
    }

    public HexBloom(Digest initial, List<Function<Digest, Digest>> hashes) {
        assert hashes.size() > 0;
        crowns = new Digest[hashes.size()];
        cardinality = 0;
        membership = new BloomFilter.DigestBloomFilter(0x666, MINIMUM_BFF_CARD, DEFAULT_FPR);
        for (int i = 0; i < crowns.length; i++) {
            crowns[i] = hashes.get(i).apply(initial);
        }
    }

    public HexBloom(HexBloome hb) {
        this(hb.getCardinality(), hb.getCrownsList().stream().map(d -> Digest.from(d)).toList(),
             BloomFilter.from(hb.getMembership()));
    }

    public HexBloom(int cardinality, List<Digest> crowns, BloomFilter<Digest> membership) {
        assert crowns.size() > 0;
        this.crowns = new Digest[crowns.size()];
        for (int i = 0; i < crowns.size(); i++) {
            this.crowns[i] = crowns.get(i);
        }
        this.membership = membership;
        this.cardinality = cardinality;
    }

    public HexBloom() {
        this(DigestAlgorithm.DEFAULT.getLast(), 1);
    }

    public HexBloom(Digest initial) {
        this(initial, 1);
    }

    /**
     * Construct a HexBloom from the supplied parameters, using default hash functions and fpr
     *
     * @param count        - the count of the currentMembership
     * @param digests      - the stream of member digests
     * @param initialCrown - the initial value of the crowns
     * @param crowns       - the number of crowns
     * @return the HexBloom built according to spec
     */
    public static HexBloom construct(int count, Stream<Digest> digests, Digest initialCrown, int crowns) {
        return construct(count, digests, Collections.emptyList(), hashes(crowns), initialCrown, DEFAULT_FPR);
    }

    /**
     * Construct a HexBloom from the supplied parameters, using default hash functions and fpr
     *
     * @param currentCount      - the count of the currentMembership
     * @param currentMembership - the stream of member digests
     * @param added             - the added member digests
     * @param initialCrown      - the initial value of the crowns
     * @param count             - the number of crowns
     * @return the HexBloom built according to spec
     */
    public static HexBloom construct(int currentCount, Stream<Digest> currentMembership, List<Digest> added,
                                     Digest initialCrown, int count) {
        return construct(currentCount, currentMembership, added, hashes(count), initialCrown, DEFAULT_FPR);
    }

    /**
     * Construct a HexBloom.
     *
     * @param currentCount      - the cardinality of the currentMembership stream
     * @param currentMembership - Stream of digests that correspond to the supplied crowns
     * @param added             - digests added that are not present in the currentMembership list
     * @param crowns            - the current crown state corresponding to the currentMembership
     * @param removed           - digests removed that are present in the currentMembership list
     * @return the HexBloom representing the new state
     */
    public static HexBloom construct(int currentCount, Stream<Digest> currentMembership, List<Digest> added,
                                     List<Digest> crowns, List<Digest> removed) {
        return construct(currentCount, currentMembership, added, crowns, removed, hashes(crowns.size()), DEFAULT_FPR);
    }

    /**
     * Construct a HexBloom.
     *
     * @param currentCount      - the cardinality of the currentMembership stream
     * @param currentMembership - Stream of digests that correspond to the supplied crowns
     * @param added             - digests added that are not present in the currentMembership list
     * @param crowns            - the current crown state corresponding to the currentMembership
     * @param removed           - digests removed that are present in the currentMembership list
     * @param hashes            - the list of functions for computing the hash of a digest for a given crown
     * @param fpr               - desired false positive rate for membership bloomfilter
     * @return the HexBloom representing the new state
     */
    public static HexBloom construct(int currentCount, Stream<Digest> currentMembership, List<Digest> added,
                                     List<Digest> crowns, List<Digest> removed, List<Function<Digest, Digest>> hashes,
                                     double fpr) {
        assert crowns.size() > 0;
        if (hashes.size() != crowns.size()) {
            throw new IllegalArgumentException(
            "Size of supplied hash functions: " + hashes.size() + " must equal the # of crowns: " + crowns.size());
        }
        var cardinality = currentCount + added.size() - removed.size();
        var membership = new BloomFilter.DigestBloomFilter(DEFAULT_SEED, Math.max(MINIMUM_BFF_CARD, cardinality), fpr);
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

    /**
     * Construct a HexBloom from the supplied parameters
     *
     * @param currentCount      - the count of the currentMembership
     * @param currentMembership - the stream of member digests
     * @param added             - the added member digests
     * @param hashes            - the supplied crown hash functions
     * @param initialCrown      - the initial value of the crowns
     * @param fpr               - the false positive rate for the membership bloom filter
     * @return the HexBloom built according to spec
     */
    public static HexBloom construct(int currentCount, Stream<Digest> currentMembership, List<Digest> added,
                                     List<Function<Digest, Digest>> hashes, Digest initialCrown, double fpr) {
        assert hashes.size() > 0;
        var cardinality = currentCount + added.size();
        var membership = new BloomFilter.DigestBloomFilter(DEFAULT_SEED, Math.max(MINIMUM_BFF_CARD, cardinality), fpr);

        var crwns = IntStream.range(0, hashes.size())
                             .mapToObj(i -> hashes.get(i).apply(initialCrown))
                             .map(d -> new AtomicReference<>(d))
                             .toList();

        currentMembership.forEach(d -> {
            for (int i = 0; i < crwns.size(); i++) {
                crwns.get(i).accumulateAndGet(hashes.get(i).apply(d), (a, b) -> a.xor(b));
            }
            membership.add(d);
        });
        added.forEach(d -> {
            for (int i = 0; i < crwns.size(); i++) {
                crwns.get(i).accumulateAndGet(hashes.get(i).apply(d), (a, b) -> a.xor(b));
            }
            membership.add(d);
        });
        return new HexBloom(cardinality, crwns.stream().map(ad -> ad.get()).toList(), membership);
    }

    /**
     * Construct a HexBloom with a membership bloomfilter using the default false positive rate and the default hash
     * transforms for each crown
     *
     * @param currentMembership - list of digests that correspond to the supplied crowns
     * @param added             - digests added that are not present in the currentMembership list
     * @param crowns            - the current crown state corresponding to the currentMembership
     * @param removed           - digests removed that are present in the currentMembership list
     * @return the HexBloom representing the new state
     */
    public static HexBloom construct(List<Digest> currentMembership, List<Digest> added, List<Digest> crowns,
                                     List<Digest> removed) {
        return construct(currentMembership, added, crowns, removed, hashes(crowns.size()), DEFAULT_FPR);
    }

    /**
     * Construct a HexBloom with a membership bloomfilter using the default false positive rate
     *
     * @param currentMembership - list of digests that correspond to the supplied crowns
     * @param added             - digests added that are not present in the currentMembership list
     * @param crowns            - the current crown state corresponding to the currentMembership
     * @param removed           - digests removed that are present in the currentMembership list
     * @param hashes            - the list of functions for computing the hash of a digest for a given crown
     * @return the HexBloom representing the new state
     */
    public static HexBloom construct(List<Digest> currentMembership, List<Digest> added, List<Digest> crowns,
                                     List<Digest> removed, List<Function<Digest, Digest>> hashes) {
        return construct(currentMembership, added, crowns, removed, hashes, DEFAULT_FPR);
    }

    /**
     * Construct a HexBloom.
     *
     * @param currentMembership - list of digests that correspond to the supplied crowns
     * @param added             - digests added that are not present in the currentMembership list
     * @param crowns            - the current crown state corresponding to the currentMembership
     * @param removed           - digests removed that are present in the currentMembership list
     * @param hashes            - the list of functions for computing the hash of a digest for a given crown
     * @param fpr               - desired false positive rate for membership bloomfilter
     * @return the HexBloom representing the new state
     */
    public static HexBloom construct(List<Digest> currentMembership, List<Digest> added, List<Digest> crowns,
                                     List<Digest> removed, List<Function<Digest, Digest>> hashes, double fpr) {
        return construct(currentMembership.size(), currentMembership.stream(), added, crowns, removed, hashes, fpr);
    }

    public static HexBloom from(HexBloome hb) {
        assert !HexBloome.getDefaultInstance().equals(hb);
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

    public HexBloom add(Digest d, List<Function<Digest, Digest>> hashes) {
        return addAll(Collections.singletonList(d), hashes);
    }

    public HexBloom addAll(List<Digest> added, List<Function<Digest, Digest>> hashes) {
        var nextCard = cardinality + added.size();
        var nextMembership = membership.clone();
        var crwns = Arrays.stream(crowns).map(AtomicReference::new).toList();

        added.forEach(d -> {
            for (int i = 0; i < crwns.size(); i++) {
                crwns.get(i).accumulateAndGet(hashes.get(i).apply(d), Digest::xor);
            }
            nextMembership.add(d);
        });

        return new HexBloom(nextCard, crwns.stream().map(AtomicReference::get).toList(), nextMembership);
    }

    public Digest compact() {
        if (crowns.length == 1) {
            return crowns[0];
        }
        return Arrays.asList(crowns).stream().reduce(crowns[0].getAlgorithm().getOrigin(), (a, b) -> a.xor(b));
    }

    /**
     * @return the hash digest of the wrapped crowns
     */
    public Digest compactWrapped() {
        return compactWrapped(hashWraps(crowns.length));
    }

    /**
     * @return the hash digest of the wrapped crowns
     */
    public Digest compactWrapped(List<Function<Digest, Digest>> hashes) {
        if (hashes.size() != crowns.length) {
            throw new IllegalArgumentException(
            "Size of supplied hash functions: " + hashes.size() + " must equal the # of crowns: " + crowns.length);
        }
        return IntStream.range(0, crowns.length)
                        .mapToObj(i -> hashes.get(i).apply(crowns[i]))
                        .toList()
                        .stream()
                        .reduce(crowns[0].getAlgorithm().getOrigin(), (a, b) -> a.xor(b));
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
     * Answer the serialized receiver, with crowns hashed using the supplied hashes functions
     *
     * @param hashes
     * @return
     */
    public HexBloome toHexBloome(List<Function<Digest, Digest>> hashes) {
        if (hashes.size() != crowns.length) {
            throw new IllegalArgumentException(
            "Size of supplied hash functions: " + hashes.size() + " must equal the # of crowns: " + crowns.length);
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

    @Override
    public String toString() {
        return "HexBloom%s".formatted(crowns().toString());
    }

    /**
     * Validate that the supplied members matches the receiver's crowns. All members must be included in the membership
     * bloomfilter, all calculated crowns must match and the cardinality must match.
     *
     * @param members - list of member digests
     * @return true if validated
     */
    public boolean validate(List<Digest> members) {
        return validate(members.stream(), hashes(crowns.length));
    }

    /**
     * Validate that the supplied members matches the receiver's crowns. All members must be included in the membership
     * bloomfilter, all calculated crowns must match and the cardinality must match.
     *
     * @param members - lsit of member digests
     * @param hashes  - hash functions for computing crowns
     * @return true if validated
     */
    public boolean validate(List<Digest> members, List<Function<Digest, Digest>> hashes) {
        return validate(members.stream(), hashes);
    }

    /**
     * Validate that the supplied members matches the receiver's crowns. All members must be included in the membership
     * bloomfilter, all calculated crowns must match and the cardinality must match.
     *
     * @param members - stream of member digests
     * @return true if validated
     */
    public boolean validate(Stream<Digest> members) {
        return validate(members, hashes(crowns.length));
    }

    /**
     * Validate that the supplied members matches the receiver's crowns. All members must be included in the membership
     * bloomfilter, all calculated crowns must match and the cardinality must match.
     *
     * @param members - stream of member digests
     * @param hashes  - hash functions for computing crowns
     * @return true if validated
     */
    public boolean validate(Stream<Digest> members, List<Function<Digest, Digest>> hashes) {
        if (hashes.size() != crowns.length) {
            throw new IllegalArgumentException(
            "Size of supplied hash functions: " + hashes.size() + " must equal the # of crowns: " + crowns.length);
        }
        var count = new AtomicInteger();
        var calculated = IntStream.range(0, crowns.length)
                                  .mapToObj(i -> new AtomicReference<Digest>(crowns[i].getAlgorithm().getOrigin()))
                                  .toList();
        members.forEach(d -> {
            for (int i = 0; i < crowns.length; i++) {
                calculated.get(i).accumulateAndGet(hashes.get(i).apply(d), (a, b) -> a.xor(b));
            }
            count.incrementAndGet();
        });
        if (count.get() != cardinality) {
            return false;
        }
        for (int i = 0; i < crowns.length; i++) {
            if (!calculated.get(i).get().equals(crowns[i])) {
                return false;
            }
        }
        return true;
    }

    public boolean validateCrown(Digest compact) {
        return validateCrown(compact, hashWraps(crowns.length));
    }

    public boolean validateCrown(Digest compact, List<Function<Digest, Digest>> hashes) {
        return compact.equals(compactWrapped(hashes));
    }

    public boolean validateCrown(List<Digest> wrapped) {
        return validateCrown(wrapped, hashWraps(crowns.length));
    }

    public boolean validateCrown(List<Digest> wrapped, List<Function<Digest, Digest>> hashes) {
        if (hashes.size() != crowns.length || wrapped.size() != crowns.length) {
            throw new IllegalArgumentException(
            "Size of supplied hash functions: " + hashes.size() + " must equal the # of crowns: " + crowns.length
            + " or wrapped: " + wrapped.size());
        }

        return IntStream.range(0, crowns.length).mapToObj(i -> hashes.get(i).apply(crowns[i])).toList().equals(wrapped);
    }

    /**
     * Answer the wrapped form of the receiver using the default wrapping hash transforms
     */
    public HexBloom wrapped() {
        return wrapped(hashWraps(crowns.length));
    }

    /**
     * Answer the wrapped form of the receiver using the supplied wrapping hash transforms
     *
     * @param hashes - the wrapping hash transforms for the crowns
     */
    public HexBloom wrapped(List<Function<Digest, Digest>> hashes) {
        if (hashes.size() != crowns.length) {
            throw new IllegalArgumentException(
            "Size of supplied hash functions: " + hashes.size() + " must equal the # of crowns: " + crowns.length);
        }
        return new HexBloom(cardinality,
                            IntStream.range(0, crowns.length).mapToObj(i -> hashes.get(i).apply(crowns[i])).toList(),
                            membership);
    }

    public List<Digest> wrappedCrowns() {
        return wrappedCrowns(hashWraps(crowns.length));
    }

    public List<Digest> wrappedCrowns(List<Function<Digest, Digest>> wrapingHash) {
        return IntStream.range(0, crowns.length).mapToObj(i -> wrapingHash.get(i).apply(crowns[i])).toList();
    }

    public static class Accumulator {
        protected final List<AtomicReference<Digest>>  accumulators;
        protected final int                            cardinality;
        protected final List<Function<Digest, Digest>> hashes;
        protected       int                            currentCount = 0;

        public Accumulator(int cardinality, int crowns, Digest initial, double fpr) {
            this(cardinality, hashes(crowns), initial, fpr);
        }

        public Accumulator(int cardinality, List<Function<Digest, Digest>> crownHashes, Digest initial, double fpr) {
            if (cardinality < 0) {
                throw new IllegalArgumentException(("Cardinality must be >= 0"));
            }
            if (crownHashes == null || crownHashes.isEmpty()) {
                throw new IllegalArgumentException("Crown hashes must not be null or empty");
            }
            if (fpr <= 0) {
                throw new IllegalArgumentException("False positive rate must be > 0");
            }
            this.cardinality = cardinality;
            this.hashes = crownHashes;
            accumulators = IntStream.range(0, hashes.size())
                                    .mapToObj(i -> hashes.get(i).apply(initial))
                                    .map(d -> new AtomicReference<>(d))
                                    .toList();
        }

        public Accumulator(int cardinality, int crowns, Digest initial) {
            this(cardinality, crowns, initial, DEFAULT_FPR);
        }

        public void add(Digest digest) {
            if (currentCount == cardinality) {
                throw new IllegalArgumentException("Current count already equal to cardinality: " + cardinality);
            }
            currentCount++;
            for (int i = 0; i < accumulators.size(); i++) {
                accumulators.get(i).accumulateAndGet(hashes.get(i).apply(digest), (a, b) -> a.xor(b));
            }
        }

        /**
         * @return the hash digest of the wrapped crowns
         */
        public Digest compactWrapped(List<Function<Digest, Digest>> hashes) {
            if (hashes.size() != accumulators.size()) {
                throw new IllegalArgumentException(
                "Size of supplied hash functions: " + hashes.size() + " must equal the # of crowns: "
                + accumulators.size());
            }
            var algorithm = accumulators.get(0).get().getAlgorithm();
            return IntStream.range(0, accumulators.size())
                            .mapToObj(i -> hashes.get(i).apply(accumulators.get(i).get()))
                            .toList()
                            .stream()
                            .reduce(algorithm.getOrigin(), (a, b) -> a.xor(b));
        }

        /**
         * @return the hash digest of the wrapped crowns
         */
        public Digest compactWrapped() {
            return compactWrapped(hashWraps(accumulators.size()));
        }

        public List<Digest> crowns() {
            return accumulators.stream().map(ar -> ar.get()).toList();
        }

        public List<Digest> wrappedCrowns() {
            return wrappedCrowns(hashWraps(accumulators.size()));
        }

        public List<Digest> wrappedCrowns(List<Function<Digest, Digest>> wrapingHash) {
            return IntStream.range(0, accumulators.size())
                            .mapToObj(i -> wrapingHash.get(i).apply(accumulators.get(i).get()))
                            .toList();
        }
    }

    public static class HexAccumulator extends Accumulator {
        private final BloomFilter<Digest> membership;

        public HexAccumulator(int cardinality, int crowns, Digest initial, double fpr) {
            this(cardinality, hashes(crowns), initial, fpr);
        }

        public HexAccumulator(int cardinality, List<Function<Digest, Digest>> crownHashes, Digest initial, double fpr) {
            super(cardinality, crownHashes, initial, fpr);
            membership = new BloomFilter.DigestBloomFilter(DEFAULT_SEED, Math.max(MINIMUM_BFF_CARD, cardinality), fpr);
        }

        public HexAccumulator(int cardinality, int crowns, Digest initial) {
            this(cardinality, crowns, initial, DEFAULT_FPR);
        }

        @Override
        public void add(Digest digest) {
            super.add(digest);
            membership.add(digest);
        }

        public HexBloom build() {
            assert currentCount == cardinality : "Did not add all members, missing: " + (cardinality - currentCount);
            return new HexBloom(cardinality, accumulators.stream().map(ar -> ar.get()).toList(), membership);
        }
    }
}
