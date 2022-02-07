/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;

/**
 * Provides a Context for Membership and is uniquely identified by a Digest;.
 * Members may be either active or offline. The Context maintains a number of
 * Rings (may be zero) that the Context provides for Firefly type consistent
 * hash ring ordering operators. Each ring has a unique hash of each individual
 * member, and thus each ring has a different ring order of the same membership
 * set. Hashes for Context level operators include the ID of the ring. Hashes
 * computed for each member, per ring include the ID of the enclosing Context.
 * 
 * @author hal.hildebrand
 *
 */
public class Context<T extends Member> {

    public interface MembershipListener<T extends Member> {

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

    private record Tracked<M> (M member, Digest[] hashes, AtomicBoolean active) {

        public String toString() {
            return String.format("%s:%s %s", member, active.get(), Arrays.asList(hashes));
        }

        private boolean isActive() {
            return active.get();
        }

        private boolean offline() {
            return active.compareAndExchange(true, false);
        }

        private boolean activate() {
            return active.compareAndExchange(false, true);
        }
    }

    private static final Logger log                = LoggerFactory.getLogger(Context.class);
    private static final String RING_HASH_TEMPLATE = "%s-%s-%s";

    public static int minMajority(double pByz, int cardinality) {
        return minMajority(pByz, cardinality, 0.99, 2);
    }

    public static int minMajority(double pByz, int card, double epsilon) {
        return minMajority(pByz, card, epsilon, 2);
    }

    /**
     * @return the minimum t such that the probability of more than t out of bias *
     *         t+1 monitors are correct with probability e/size given the uniform
     *         probability pByz that a monitor is Byzantine.
     */
    public static int minMajority(double pByz, int cardinality, double epsilon, int bias) {
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
    public static int minMajority(int bias, double pByz, int cardinality) {
        return minMajority(pByz, cardinality, 0.99, bias);
    }

    private final int                              bias;
    private final Digest                           id;
    private final Map<Digest, Tracked<T>>          members             = new ConcurrentHashMap<>();
    private final Map<UUID, MembershipListener<T>> membershipListeners = new ConcurrentHashMap<>();
    private final double                           pByz;
    private final List<Ring<T>>                    rings               = new ArrayList<>();

    public Context(Digest id) {
        this(id, 1);
    }

    /**
     * Construct a context with the given id and cardinality where the number of
     * rings is 2 * T + 1, where T is the tolerance level. The tolerance level is
     * calculated by the minMajority of the input probability of any member being
     * byzantine and the epsilon indicating how close to probability 1 that a member
     * will not be unfortunate
     * 
     * @param id
     * @param pByz
     * @param cardinality
     */
    public Context(Digest id, double pByz, int cardinality) {
        this(pByz, 2, id, minMajority(pByz, cardinality) * 2 + 1);
    }

    /**
     * Construct a context with the given id and cardinality where the number of
     * rings is 2 * T + 1, where T is the tolerance level. The tolerance level is
     * calculated by the minMajority of the input probability of any member being
     * byzantine and the epsilon indicating how close to probability 1 that a member
     * will not be unfortunate
     * 
     * @param id
     * @param pByz
     * @param cardinality
     * @param epsilon
     */
    public Context(Digest id, double pByz, int cardinality, double epsilon, int bias) {
        this(pByz, bias, id, minMajority(pByz, cardinality, epsilon, bias) * bias + 1);
    }

    public Context(Digest id, double pByz, int cardinality, int bias) {
        this(pByz, bias, id, minMajority(pByz, cardinality, 0.99, bias) * bias + 1);
    }

    public Context(Digest id, int r) {
        this(0.0, 2, id, r);
    }

    public Context(double pbyz, int bias, Digest id, int r) {
        this.pByz = pbyz;
        this.id = id;
        this.bias = bias;
        for (int i = 0; i < r; i++) {
            rings.add(new Ring<T>(i, (m, ring) -> hashFor(m, ring)));
        }
    }

    public void activate(Collection<T> activeMembers) {
        activeMembers.forEach(m -> activate(m));
    }

    /**
     * Mark a member as active in the context
     */
    public void activate(T m) {
        if (tracking(m).activate()) {
            membershipListeners.values().stream().forEach(l -> {
                try {
                    l.active(m);
                } catch (Throwable e) {
                    log.error("error recoving member in listener: " + l, e);
                }
            });
        }
    }

    /**
     * Mark a member as active in the context
     */
    public void activateIfMember(T m) {
        var member = members.get(m.getId());
        if (member != null && member.activate()) {
            membershipListeners.values().stream().forEach(l -> {
                try {
                    l.active(m);
                } catch (Throwable e) {
                    log.error("error recoving member in listener: " + l, e);
                }
            });
        }
    }

    public int activeCount() {
        return (int) members.values().stream().filter(e -> e.isActive()).count();
    }

    public List<T> activeMembers() {
        return members.values().stream().filter(e -> e.isActive()).map(e -> e.member).toList();
    }

    public void add(Collection<T> members) {
        members.forEach(m -> add(m));
    }

    public void add(T m) {
        offline(m);
    }

    public Stream<T> allMembers() {
        return members.values().stream().map(e -> e.member);
    }

    public int cardinality() {
        return members.size();
    }

    public void clear() {
        for (Ring<T> ring : rings) {
            ring.clear();
        }
        members.clear();
    }

    public UUID dependUpon(Context<T> foundation) {
        return foundation.register(new MembershipListener<T>() {
            @Override
            public void active(T member) {
                activateIfMember(member);
            }

            @Override
            public void offline(T member) {
                offlineIfMember(member);
            }
        });
    }

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built
     * with FF parameters, with the rings forming random graph connections segments.
     */
    public int diameter() {
        return diameter(cardinality());
    }

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built
     * with FF parameters, with the rings forming random graph connections segments
     * with the supplied cardinality
     */
    public int diameter(int c) {
        double pN = ((double) (2 * toleranceLevel())) / ((double) c);
        double logN = Math.log(c);
        return (int) (logN / Math.log(c * pN));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if ((obj == null) || (getClass() != obj.getClass()))
            return false;
        Context<?> other = (Context<?>) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    public Collection<T> getActive() {
        return activeMembers();
    }

    public T getActiveMember(Digest memberID) {
        var tracked = members.get(memberID);
        return tracked == null ? null : tracked.member;
    }

    public int getBias() {
        return bias;
    }

    public Digest getId() {
        return id;
    }

    public T getMember(Digest memberID) {
        var tracked = members.get(memberID);
        return tracked == null ? null : tracked.member;
    }

    public Collection<T> getOffline() {
        return members.values().stream().filter(e -> !e.isActive()).map(e -> e.member).toList();
    }

    public double getProbabilityByzantine() {
        return pByz;
    }

    public int getRingCount() {
        return rings.size();
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public boolean isActive(Digest id) {
        assert id != null;
        var member = members.get(id);
        if (member == null) {
            return false;
        }
        return member.isActive();
    }

    public boolean isActive(T m) {
        assert m != null;
        return isActive(m.getId());
    }

    public boolean isOffline(Digest digest) {
        assert digest != null;
        var member = members.get(digest);
        if (member == null) {
            return true;
        }
        return !member.isActive();
    }

    public boolean isOffline(T m) {
        assert m != null;
        return isOffline(m.getId());
    }

    public int majority() {
        return getRingCount() - toleranceLevel();
    }

    public int memberCount() {
        return members.size();
    }

    public void offline(Collection<T> members) {
        members.forEach(m -> offline(m));
    }

    /**
     * Take a member offline
     */
    public void offline(T m) {
        if (tracking(m).offline()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.offline(m);
                } catch (Throwable e) {
                    log.error("error sending fail to listener: " + l, e);
                }
            });
        }
    }

    public int offlineCount() {
        return (int) members.values().stream().filter(e -> !e.isActive()).count();
    }

    /**
     * Take a member offline if already a member
     */
    public void offlineIfMember(T m) {
        var member = members.get(m.getId());
        if (member != null && member.offline()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.offline(m);
                } catch (Throwable e) {
                    log.error("error offlining member in listener: " + l, e);
                }
            });
        }
    }

    /**
     * @return the predecessor on each ring for the provided key
     */
    public List<T> predecessors(Digest key) {
        return predecessors(key, t -> true);
    }

    /**
     * @return the predecessor on each ring for the provided key that pass the
     *         provided predicate
     */
    public List<T> predecessors(Digest key, Predicate<T> test) {
        List<T> predecessors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            T predecessor = ring.predecessor(key, test);
            if (predecessor != null) {
                predecessors.add(predecessor);
            }
        }
        return predecessors;
    }

    /**
     * @return the predecessor on each ring for the provided key
     */
    public List<T> predecessors(T key) {
        return predecessors(key, t -> true);
    }

    /**
     * @return the predecessor on each ring for the provided key that pass the
     *         provided predicate
     */
    public List<T> predecessors(T key, Predicate<T> test) {
        List<T> predecessors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            T predecessor = ring.predecessor(key, test);
            if (predecessor != null) {
                predecessors.add(predecessor);
            }
        }
        return predecessors;
    }

    public UUID register(MembershipListener<T> listener) {
        var uuid = UUID.randomUUID();
        membershipListeners.put(uuid, listener);
        return uuid;
    }

    public void remove(Collection<T> members) {
        members.forEach(m -> remove(m));
    }

    /**
     * remove a member from the receiving Context
     */
    public void remove(T m) {
        members.remove(m.getId());
        for (int i = 0; i < rings.size(); i++) {
            rings.get(i).delete(m);
        }
    }

    /**
     * @return the indexed Ring<T>
     */
    public Ring<T> ring(int index) {
        if (index < 0 || index >= rings.size()) {
            throw new IllegalArgumentException("Not a valid ring #: " + index);
        }
        return rings.get(index);
    }

    /**
     * @return the Stream of rings managed by the context
     */
    public Stream<Ring<T>> rings() {
        return rings.stream();
    }

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
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Digest exc) {
        Member excluded = getMember(exc);
        return rings.get(entropy.nextInt(rings.size()))
                    .stream()
                    .collect(new ReservoirSampler<T>(excluded, range, entropy));
    }

    /**
     * @return the list of successors to the key on each ring
     */
    public List<T> successors(Digest key) {
        return successors(key, t -> true);
    }

    /**
     * @return the list of successor to the key on each ring that pass the provided
     *         predicate test
     */
    public List<T> successors(Digest key, Predicate<T> test) {
        List<T> successors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            T successor = ring.successor(key, test);
            if (successor != null) {
                successors.add(successor);
            }
        }
        return successors;
    }

    /**
     * @return the list of successors to the key on each ring
     */
    public List<T> successors(T key) {
        return successors(key, t -> true);
    }

    /**
     * @return the list of successor to the key on each ring that pass the provided
     *         predicate test
     */
    public List<T> successors(T key, Predicate<T> test) {
        List<T> successors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            T successor = ring.successor(key, test);
            if (successor != null) {
                successors.add(successor);
            }
        }
        return successors;
    }

    /**
     * The number of iterations until a given message has been distributed to all
     * members in the context, using the rings of the receiver as a gossip graph
     */
    public int timeToLive() {
        return (rings.size() * diameter()) + 1;
    }

    /**
     * Answer the tolerance level of the context to byzantine members, assuming this
     * context has been constructed from FF parameters
     */
    public int toleranceLevel() {
        return (rings.size() - 1) / 2;
    }

    @Override
    public String toString() {
        return "Context [id=" + id + " " + ring(0) + "]";
    }

    Digest hashFor(T m, int index) {
        var tracked = members.get(m.getId());
        if (tracked == null) {
            throw new IllegalArgumentException(m + " is not part of this group " + id);
        }
        return tracked.hashes[index];
    }

    private Digest[] hashesFor(T m) {
        Digest[] s = new Digest[rings.size()];
        for (int ring = 0; ring < rings.size(); ring++) {
            Digest key = m.getId();
            s[ring] = key.getAlgorithm()
                         .digest(String.format(RING_HASH_TEMPLATE, qb64(id), qb64(key), ring).getBytes());
        }
        return s;
    }

    private Tracked<T> tracking(T m) {
        var added = new AtomicBoolean();
        var member = members.computeIfAbsent(m.getId(), id -> {
            added.set(true);
            return new Tracked<>(m, hashesFor(m), new AtomicBoolean());
        });
        if (added.get()) {
            for (var ring : rings) {
                ring.insert(m);
            }
        }
        return member;
    }
}
