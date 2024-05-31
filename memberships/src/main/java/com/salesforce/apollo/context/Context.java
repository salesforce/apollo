/*
 * Copyright (c) 2024, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.context;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Util;
import org.apache.commons.math3.random.BitsStreamGenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Provides a Context for Membership and is uniquely identified by a Digest. Members may be either active or offline.
 * The Context maintains a number of Rings (can be zero) that the Context provides for Firefly type consistent hash ring
 * ordering operators. Each ring has a unique hash of each member, and thus each ring has a different ring order of the
 * same membership set. Hashes for DynamicContext level operators include the ID of the ring. Hashes computed and cached
 * for each member per ring include the ID of the enclosing DynamicContext.
 *
 * @param <T>
 * @author hal.hildebrand
 */
public interface Context<T extends Member> {

    double DEFAULT_EPSILON = 0.99999;

    static List<Member> uniqueSuccessors(Context<Member> context, Digest digest) {
        Set<Member> post = new HashSet<>();
        context.successors(digest, m -> {
            if (post.size() == context.getRingCount()) {
                return false;
            }
            return post.add(m);
        });
        var successors = new ArrayList<>(post);
        return successors;
    }

    static Digest hashFor(Digest ctxId, int ring, Digest d) {
        return d.prefix(ctxId, ring);
    }

    static int majority(int rings, int bias) {
        return (bias - 1) * toleranceLevel(rings, bias);
    }

    static int toleranceLevel(int rings, int bias) {
        return ((rings - 1) / bias);
    }

    static short minimalQuorum(int np, double bias) {
        return (short) (toleranceLevel(np, (int) bias) + 1);
    }

    static int minMajority(double pByz, int cardinality) {
        return minMajority(pByz, cardinality, 0.99999, 2);
    }

    static int minMajority(double pByz, int card, double epsilon) {
        return minMajority(pByz, card, epsilon, 2);
    }

    /**
     * @return the minimum t such that the probability of more than t out of bias * t+1 monitors is correct with
     * probability e/size given the uniform probability pByz that a monitor is Byzantine.
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
                    throw new IllegalArgumentException(
                    "Cardinality: " + cardinality + " cannot support required tolerance: " + t);
                }
            }
        }
        throw new IllegalArgumentException(
        "Cannot compute number of rings from bias=" + bias + " pByz=" + pByz + " cardinality: " + cardinality
        + " epsilon: " + epsilon);
    }

    /**
     * @return the minimum t such that the probability of more than t out of 2t+1 monitors are correct with probability
     * e/size given the uniform probability pByz that a monitor is Byzantine.
     */
    static int minMajority(int bias, double pByz, int cardinality) {
        return minMajority(pByz, cardinality, 0.99999, bias);
    }

    /**
     * Answer a stream over all members, offline and active
     */
    Stream<T> allMembers();

    /**
     * @param start
     * @param stop
     * @return Return all counter-clockwise items between (but not including) start and stop
     */
    Iterable<T> betweenPredecessors(int ring, T start, T stop);

    /**
     * @param start
     * @param stop
     * @return all clockwise items between (but not including) start item and stop item.
     */
    Iterable<T> betweenSuccessor(int ring, T start, T stop);

    /**
     * @param hash - the point on the rings to determine successors
     * @return the Set of Members constructed from the sucessors of the supplied hash on each of the receiver Context's
     * rings
     */
    default Set<Member> bftSubset(Digest hash) {
        Set<Member> successors = new HashSet<>();
        successors(hash, m -> {
            if (successors.size() == getRingCount()) {
                return false;
            }
            return successors.add(m);
        });
        return successors;
    }

    /**
     * Maximum cardinality of this context
     */
    int cardinality();

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built with FF parameters, with the rings
     * forming random graph connections segments.
     */
    default int diameter() {
        double pN = ((double) (getBias() * toleranceLevel())) / ((double) cardinality());
        double logN = Math.log(cardinality());
        return (int) (logN / Math.log(cardinality() * pN));
    }

    /**
     * @param d         - the digest
     * @param predicate - the test function.
     * @return the first successor of d for which function evaluates to SUCCESS. Answer null if function evaluates to
     * FAIL.
     */
    T findPredecessor(int ring, Digest d, Function<T, IterateResult> predicate);

    /**
     * @param m         - the member
     * @param predicate - the test function.
     * @return the first successor of m for which function evaluates to SUCCESS. Answer null if function evaluates to
     * FAIL.
     */
    T findPredecessor(int ring, T m, Function<T, IterateResult> predicate);

    /**
     * @param d         - the digest
     * @param predicate - the test function.
     * @return the first successor of d for which function evaluates to SUCCESS. Answer null if function evaluates to
     * FAIL.
     */
    T findSuccessor(int ring, Digest d, Function<T, IterateResult> predicate);

    /**
     * @param m         - the member
     * @param predicate - the test function.
     * @return the first successor of m for which function evaluates to SUCCESS. Answer null if function evaluates to
     * FAIL.
     */
    T findSuccessor(int ring, T m, Function<T, IterateResult> predicate);

    /**
     * @return the List of all members
     */
    List<T> getAllMembers();

    /**
     * Answer the bias of the context. The bias is the multiple of the number of byzantine members the context is
     * designed to foil
     */
    int getBias();

    double getEpsilon();

    /**
     * Answer the identifier of the context
     */
    Digest getId();

    /**
     * Answer the member matching the id, or null if none.
     */
    T getMember(Digest memberID);

    /**
     * @param i
     * @param ring
     * @return the i'th Member in Ring 0 of the receiver
     */
    T getMember(int i, int ring);

    /**
     * Answer the probability {0, 1} that any given member is byzantine
     */
    double getProbabilityByzantine();

    /**
     * Answer the number of rings in the context
     */
    short getRingCount();

    default Digest hashFor(Digest d, int ring) {
        return hashFor(getId(), ring, d);
    }

    Digest hashFor(T m, int ring);

    /**
     * <pre>
     *
     *    - An item lies between itself. That is, if pred == itm == succ, True is
     *    returned.
     *
     *    - Everything lies between an item and item and itself. That is, if pred == succ, then
     *    this method always returns true.
     *
     *    - An item is always between itself and any other item. That is, if
     *    pred == item, or succ == item, this method returns True.
     * </pre>
     *
     * @param predecessor - the asserted predecessor on the ring
     * @param item        - the item to test
     * @param successor   - the asserted successor on the ring
     * @return true if the member item is between the pred and succ members on the ring
     */
    boolean isBetween(int ring, T predecessor, T item, T successor);

    /**
     * Answer true if a member who's id is the supplied digest is a member of the view
     */
    boolean isMember(Digest digest);

    /**
     * Answer true if is a member of the view
     */
    boolean isMember(T m);

    /**
     * Answer true if the member is a successor of the supplied digest on any ring
     *
     * @param m
     * @param digest
     * @return
     */
    boolean isSuccessorOf(T m, Digest digest);

    /**
     * Answer the majority cardinality of the context, based on the current ring count
     */
    default int majority() {
        return getRingCount() - toleranceLevel();
    }

    /**
     * Answer the total member count (offline + active) tracked by this context
     */
    int memberCount();

    T predecessor(int ring, Digest location);

    /**
     * @param location  - the target
     * @param predicate - the test predicate
     * @return the first predecessor of m for which predicate evaluates to True. m is never evaluated.
     */
    T predecessor(int ring, Digest location, Predicate<T> predicate);

    /**
     * @param m - the member
     * @return the predecessor of the member
     */
    T predecessor(int ring, T m);

    /**
     * @param m         - the member
     * @param predicate - the test predicate
     * @return the first predecessor of m for which predicate evaluates to True. m is never evaluated.
     */
    T predecessor(int ring, T m, Predicate<T> predicate);

    /**
     * @return the predecessor on each ring for the provided key
     */
    List<T> predecessors(Digest key);

    /**
     * @return the predecessor on each ring for the provided key that pass the provided predicate
     */
    List<T> predecessors(Digest key, Predicate<T> test);

    /**
     * @return the predecessor on each ring for the provided key
     */
    List<T> predecessors(T key);

    /**
     * @return the predecessor on each ring for the provided key that pass the provided predicate
     */
    List<T> predecessors(T key, Predicate<T> test);

    Iterable<T> predecessors(int ring, Digest location);

    /**
     * @param location
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
     * excluding) the first item where predicate(item) evaluates to True.
     */
    Iterable<T> predecessors(int ring, Digest location, Predicate<T> predicate);

    Iterable<T> predecessors(int ring, T start);

    /**
     * @param start
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
     * excluding) the first item where predicate(item) evaluates to True.
     */
    Iterable<T> predecessors(int ring, T start, Predicate<T> predicate);

    /**
     * @return the number of items between item and dest
     */
    int rank(int ring, Digest item, Digest dest);

    /**
     * @return the number of items between item and dest
     */
    int rank(int ring, Digest item, T dest);

    /**
     * @return the number of items between item and dest
     */
    int rank(int ring, T item, T dest);

    /**
     * Answer a random sample of at least range size from the active members of the context
     *
     * @param range   - the desired range
     * @param entropy - source o randomness
     * @param exc     - the member to exclude from sample
     * @return a random sample set of the view's live members. May be limited by the number of active members.
     */
    <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Digest exc);

    /**
     * Answer a random sample of at least range size from the active members of the context
     *
     * @param range    - the desired range
     * @param entropy  - source o randomness
     * @param excluded - the member to exclude from sample
     * @return a random sample set of the view's live members. May be limited by the number of active members.
     */
    <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Predicate<T> excluded);

    /**
     * Answer the total count of active and offline members of this context
     */
    int size();

    /**
     * Stream the members of the ring in hashed order
     *
     * @param ring
     * @return
     */
    Stream<T> stream(int ring);

    /**
     * @param ring
     * @param predicate
     * @return a Stream of all items counter-clock wise in the ring from (but excluding) start location to (but
     * excluding) the first item where predicate(item) evaluates to True.
     */
    Stream<T> streamPredecessors(int ring, Digest location, Predicate<T> predicate);

    /**
     * @param ring
     * @param predicate
     * @return a list of all items counter-clock wise in the ring from (but excluding) start item to (but excluding) the
     * first item where predicate(item) evaluates to True.
     */
    Stream<T> streamPredecessors(int ring, T m, Predicate<T> predicate);

    /**
     * @param ring
     * @param predicate
     * @return a Stream of all items counter-clock wise in the ring from (but excluding) start location to (but
     * excluding) the first item where predicate(item) evaluates to True.
     */
    Stream<T> streamSuccessors(int ring, Digest location, Predicate<T> predicate);

    /**
     * @param ring
     * @param predicate
     * @return a Stream of all items counter-clock wise in the ring from (but excluding) start item to (but excluding)
     * the first item where predicate(item) evaluates to True.
     */
    Stream<T> streamSuccessors(int ring, T m, Predicate<T> predicate);

    /**
     * @param ring
     * @return a iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
     * excluding) the first item
     */
    T successor(int ring, Digest hash);

    /**
     * @param hash      - the location to start on the ring
     * @param predicate - the test predicate
     * @return the first successor of m for which predicate evaluates to True. m is never evaluated..
     */
    T successor(int ring, Digest hash, Predicate<T> predicate);

    /**
     * @param m - the member
     * @return the successor of the member
     */
    T successor(int ring, T m);

    /**
     * @param m         - the member
     * @param predicate - the test predicate
     * @return the first successor of m for which predicate evaluates to True. m is never evaluated..
     */
    T successor(int ring, T m, Predicate<T> predicate);

    /**
     * @return the list of successors to the key on each ring
     */
    List<T> successors(Digest key);

    /**
     * @return the list of successor to the key on each ring that pass the provided predicate test
     */
    List<T> successors(Digest key, Predicate<T> test);

    /**
     * @return the list of successors to the key on each ring
     */
    List<T> successors(T key);

    /**
     * @return the list of successor to the key on each ring that pass the provided predicate test
     */
    List<T> successors(T key, Predicate<T> test);

    Iterable<T> successors(int ring, Digest location);

    /**
     * @param ring
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start location to (but
     * excluding) the first item where predicate(item) evaluates to True.
     */
    Iterable<T> successors(int ring, Digest location, Predicate<T> predicate);

    /**
     * @param ring
     * @param predicate
     * @return an Iterable of all items counter-clock wise in the ring from (but excluding) start item to (but
     * excluding) the first item where predicate(item) evaluates to True.
     */
    Iterable<T> successors(int ring, T m, Predicate<T> predicate);

    /**
     * The number of iterations until a given message has been distributed to all members in the context, using the
     * rings of the receiver as a gossip graph
     */
    default int timeToLive() {
        return (getRingCount() * diameter()) + 1;
    }

    /**
     * Answer the tolerance level of the context to byzantine members, assuming this context has been constructed from
     * FF parameters
     */
    default int toleranceLevel() {
        return (getRingCount() - 1) / getBias();
    }

    /**
     * @param member
     * @return the iteratator to traverse the ring starting at the member
     */
    Iterable<T> traverse(int ring, T member);

    boolean validRing(int ring);

    /**
     * @author hal.hildebrand
     **/
    enum IterateResult {
        CONTINUE, FAIL, SUCCESS
    }

    /**
     * @author hal.hildebrand
     **/
    abstract class Builder<Z extends Member> {
        protected int    bias    = 2;
        protected int    cardinality;
        protected double epsilon = DEFAULT_EPSILON;
        protected Digest id      = DigestAlgorithm.DEFAULT.getOrigin();
        protected double pByz    = 0.1;                                // 10% chance any node is out to get ya

        public abstract <C extends Context<Z>> C build();

        public int getBias() {
            return bias;
        }

        public Builder<Z> setBias(int bias) {
            this.bias = bias;
            return this;
        }

        public int getCardinality() {
            return cardinality;
        }

        public Builder<Z> setCardinality(int cardinality) {
            this.cardinality = cardinality;
            return this;
        }

        public double getEpsilon() {
            return epsilon;
        }

        public Builder<Z> setEpsilon(double epsilon) {
            this.epsilon = epsilon;
            return this;
        }

        public Digest getId() {
            return id;
        }

        public Builder<Z> setId(Digest id) {
            this.id = id;
            return this;
        }

        public double getpByz() {
            return pByz;
        }

        public Builder<Z> setpByz(double pByz) {
            this.pByz = pByz;
            return this;
        }
    }
}
