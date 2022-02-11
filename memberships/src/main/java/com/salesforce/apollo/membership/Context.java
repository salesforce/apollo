/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.math3.random.BitsStreamGenerator;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * Provides a Context for Membership and is uniquely identified by a Digest;.
 * Members may be either active or offline. The Context maintains a number of
 * Rings (may be zero) that the Context provides for Firefly type consistent
 * hash ring ordering operators. Each ring has a unique hash of each individual
 * member, and thus each ring has a different ring order of the same membership
 * set. Hashes for Context level operators include the ID of the ring. Hashes
 * computed and cached for each member, per ring include the ID of the enclosing
 * Context.
 * 
 * @author hal.hildebrand
 *
 */
public interface Context<T extends Member> {

    abstract class Builder<Z extends Member, Q extends Context<Z>> {
        protected int    bias    = 2;
        protected int    cardinality;
        protected double epsilon = 0.01;
        protected Digest id      = DigestAlgorithm.DEFAULT.getOrigin();
        protected double pByz    = 0.1;                                // 10% chance any node is out to get ya

        public abstract Q build();

        public int getBias() {
            return bias;
        }

        public int getCardinality() {
            return cardinality;
        }

        public double getEpsilon() {
            return epsilon;
        }

        public Digest getId() {
            return id;
        }

        public double getpByz() {
            return pByz;
        }

        public Builder<Z, Q> setBias(int bias) {
            this.bias = bias;
            return this;
        }

        public Builder<Z, Q> setCardinality(int cardinality) {
            this.cardinality = cardinality;
            return this;
        }

        public Builder<Z, Q> setEpsilon(double epsilon) {
            this.epsilon = epsilon;
            return this;
        }

        public Builder<Z, Q> setId(Digest id) {
            this.id = id;
            return this;
        }

        public Builder<Z, Q> setpByz(double pByz) {
            this.pByz = pByz;
            return this;
        }
    }

    interface MembershipListener<T extends Member> {

        /**
         * A new member has recovered and is now active
         * 
         * @param member
         */
        default void active(T member) {
        };

        /**
         * A member is offline
         * 
         * @param member
         */
        default void offline(T member) {
        };
    }

    record Tracked<M> (M member, Digest[] hashes, AtomicBoolean active) {

        @Override
        public String toString() {
            return String.format("%s:%s %s", member, active.get(), Arrays.asList(hashes));
        }

        public boolean isActive() {
            return active.get();
        }

        public boolean offline() {
            return active.compareAndExchange(true, false);
        }

        public boolean activate() {
            return active.compareAndExchange(false, true);
        }

        public Digest hash(int index) {
            return hashes[index];
        }
    }

    static final String RING_HASH_TEMPLATE = "%s-%s-%s";

    static int minMajority(double pByz, int cardinality) {
        return minMajority(pByz, cardinality, 0.99, 2);
    }

    static int minMajority(double pByz, int card, double epsilon) {
        return minMajority(pByz, card, epsilon, 2);
    }

    /**
     * @return the minimum t such that the probability of more than t out of bias *
     *         t+1 monitors are correct with probability e/size given the uniform
     *         probability pByz that a monitor is Byzantine.
     */
    static int minMajority(double pByz, int cardinality, double epsilon, int bias) {
        if (epsilon > 1.0 || epsilon <= 0.0) {
            throw new IllegalArgumentException("epsilon must be > 0 and <= 1 : " + epsilon);
        }
        double e = epsilon / cardinality;
        for (int t = 1; t <= ((cardinality - 1) / bias); t++) {
            double pf = 1.0 - Util.binomialc(t, (bias * t) + 1, pByz);
            if (e >= pf) {
                if (cardinality >= (bias * t) + 1) {
                    return t;
                } else {
                    throw new IllegalArgumentException("Cardinality: " + cardinality
                    + " cannot support required tolerance: " + t);
                }
            }
        }
        throw new IllegalArgumentException("Cannot compute number of rings from pByz=" + pByz + " cardinality: "
        + cardinality + " epsilon: " + epsilon);
    }

    /**
     * @return the minimum t such that the probability of more than t out of 2t+1
     *         monitors are correct with probability e/size given the uniform
     *         probability pByz that a monitor is Byzantine.
     */
    static int minMajority(int bias, double pByz, int cardinality) {
        return minMajority(pByz, cardinality, 0.99, bias);
    }

    static <Z extends Member, Q extends Context<Z>> Builder<Z, Q> newBuilder() {
        return new Builder<Z, Q>() {

            @Override
            public Q build() {
                @SuppressWarnings("unchecked")
                var ctx = (Q) new ContextImpl<Z>(pByz, bias, id,
                                                 minMajority(pByz, cardinality, epsilon, bias) * bias + 1);
                return ctx;
            }
        };
    }

    /**
     * Activate the supplied collection of members
     */
    void activate(Collection<T> activeMembers);

    /**
     * Mark a member as active in the context
     */
    void activate(T m);

    /**
     * Mark a member as active in the context
     */
    void activateIfMember(T m);

    /**
     * Answer the count of active members
     */
    int activeCount();

    /**
     * Answer the list of active members
     */
    List<T> activeMembers();

    /**
     * Add a collection of members in the offline state
     */
    void add(Collection<T> members);

    /**
     * Add a member in the offline state
     */
    void add(T m);

    /**
     * Answer a stream over all members, offline and active
     */
    Stream<T> allMembers();

    /**
     * Clear all members from the receiver
     */
    void clear();

    /**
     * Link the lifecycle of member in receiver context with the foundation
     */
    UUID dependUpon(Context<T> foundation);

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built
     * with FF parameters, with the rings forming random graph connections segments.
     */
    int diameter();

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built
     * with FF parameters, with the rings forming random graph connections segments
     * with the supplied cardinality
     */
    int diameter(int c);

    /**
     * Answer the collection of active members
     */
    Collection<T> getActive();

    /**
     * Answer the active member having the id, or null if offline or non-existent
     */
    T getActiveMember(Digest memberID);

    /**
     * Answer the bias of the context. The bias is the multiple of the number of
     * byzantine members the context is designed to foil
     */
    int getBias();

    /**
     * Answer the identifier of the context
     */
    Digest getId();

    /**
     * Answer the member matching the id, or null if none.
     */
    T getMember(Digest memberID);

    /**
     * Answer the collection of offline members
     */
    Collection<T> getOffline();

    /**
     * Answer the probability {0, 1} that any given member is byzantine
     */
    double getProbabilityByzantine();

    /**
     * Answer the number of rings in the context
     */
    int getRingCount();

    Digest hashFor(T m, int index);

    /**
     * Answer true if the member who's id is active
     */
    boolean isActive(Digest id);

    /**
     * Answer true if the member is active
     */
    boolean isActive(T m);

    /**
     * Answer true if a member who's id is the supplied digest is offline
     */
    boolean isOffline(Digest digest);

    /**
     * Answer true if a member is offline
     */
    boolean isOffline(T m);

    /**
     * Answer the majority cardinality of the context, based on the current ring
     * count
     */
    int majority();

    /**
     * Answer the total member count (offline + active) tracked by this context
     */
    int memberCount();

    /**
     * Take the collection of members offline
     */
    void offline(Collection<T> members);

    /**
     * Take a member offline
     */
    void offline(T m);

    int offlineCount();

    /**
     * Take a member offline if already a member
     */
    void offlineIfMember(T m);

    /**
     * @return the predecessor on each ring for the provided key
     */
    List<T> predecessors(Digest key);

    /**
     * @return the predecessor on each ring for the provided key that pass the
     *         provided predicate
     */
    List<T> predecessors(Digest key, Predicate<T> test);

    /**
     * @return the predecessor on each ring for the provided key
     */
    List<T> predecessors(T key);

    /**
     * @return the predecessor on each ring for the provided key that pass the
     *         provided predicate
     */
    List<T> predecessors(T key, Predicate<T> test);

    /**
     * Register a listener for membership events, answer the UUID that identifies it
     */
    UUID register(MembershipListener<T> listener);

    /**
     * Remove the members from the context
     */
    void remove(Collection<T> members);

    /**
     * remove a member from the receiving Context
     */
    void remove(T m);

    /**
     * @return the indexed Ring<T>
     */
    Ring<T> ring(int index);

    /**
     * @return the Stream of rings managed by the context
     */
    Stream<Ring<T>> rings();

    /**
     * Answer a random sample of at least range size from the active members of the
     * context
     * 
     * @param range    - the desired range
     * @param entropy  - source o randomness
     * @param excluded - the member to exclude from sample
     * @return a random sample set of the view's live members. May be limited by the
     *         number of active members.
     */
    <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Digest exc);

    /**
     * Answer the total count of active and offline members of this context
     */
    int size();

    /**
     * @return the list of successors to the key on each ring
     */
    List<T> successors(Digest key);

    /**
     * @return the list of successor to the key on each ring that pass the provided
     *         predicate test
     */
    List<T> successors(Digest key, Predicate<T> test);

    /**
     * @return the list of successors to the key on each ring
     */
    List<T> successors(T key);

    /**
     * @return the list of successor to the key on each ring that pass the provided
     *         predicate test
     */
    List<T> successors(T key, Predicate<T> test);

    /**
     * The number of iterations until a given message has been distributed to all
     * members in the context, using the rings of the receiver as a gossip graph
     */
    int timeToLive();

    /**
     * Answer the tolerance level of the context to byzantine members, assuming this
     * context has been constructed from FF parameters
     */
    int toleranceLevel();

}
