/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import com.salesforce.apollo.cryptography.Digest;
import org.apache.commons.math3.random.BitsStreamGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.salesforce.apollo.membership.Context.minMajority;

/**
 * Provides a DynamicContext for Membership and is uniquely identified by a Digest. Members may be either active or
 * offline. The DynamicContext maintains a number of Rings (may be zero) that the DynamicContext provides for Firefly
 * type consistent hash ring ordering operators. Each ring has a unique hash of each individual member, and thus each
 * ring has a different ring order of the same membership set. Hashes for DynamicContext level operators include the ID
 * of the ring. Hashes computed for each member, per ring include the ID of the enclosing DynamicContext.
 *
 * @author hal.hildebrand
 */
public class DynamicContextImpl<T extends Member> implements DynamicContext<T> {

    private static final Logger log = LoggerFactory.getLogger(DynamicContext.class);

    private final    int                              bias;
    private final    double                           epsilon;
    private final    Digest                           id;
    private final    Map<Digest, Tracked<T>>          members             = new ConcurrentSkipListMap<>();
    private final    Map<UUID, MembershipListener<T>> membershipListeners = new ConcurrentHashMap<>();
    private final    double                           pByz;
    private final    List<Ring<T>>                    rings               = new ArrayList<>();
    private volatile int                              cardinality;

    public DynamicContextImpl(Digest id, int cardinality, double pbyz, int bias) {
        this(id, cardinality, pbyz, bias, DEFAULT_EPSILON);
    }

    public DynamicContextImpl(Digest id, int cardinality, double pbyz, int bias, double epsilon) {
        this.pByz = pbyz;
        this.id = id;
        this.bias = bias;
        this.cardinality = cardinality;
        this.epsilon = epsilon;
        for (int i = 0; i < (minMajority(pByz, cardinality, epsilon, bias) * bias) + 1; i++) {
            rings.add(new Ring<>(i, this));
        }
    }

    @Override
    public void activate(Collection<T> activeMembers) {
        activeMembers.forEach(this::activate);
    }

    /**
     * Mark a member as active in the context
     */
    @Override
    public boolean activate(T m) {
        if (tracking(m).activate()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.active(m);
                } catch (Throwable e) {
                    log.error("error recoving member in listener: " + l, e);
                }
            });
            return true;
        }
        return false;
    }

    /**
     * Mark a member identified by the digest ID as active in the context
     *
     * @return true if the member was previously inactive, false if currently active
     */
    public boolean activate(Digest id) {
        var m = members.get(id);
        if (m == null) {
            throw new NoSuchElementException("No member known: " + id);
        }
        if (m.activate()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.active(m.member);
                } catch (Throwable e) {
                    log.error("error recovering member in listener: " + l, e);
                }
            });
            return true;
        }
        return false;
    }

    /**
     * Mark a member as active in the context
     */
    @Override
    public boolean activateIfMember(T m) {
        var member = members.get(m.getId());
        if (member != null && member.activate()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.active(m);
                } catch (Throwable e) {
                    log.error("error recovering member in listener: " + l, e);
                }
            });
            return true;
        }
        return false;
    }

    @Override
    public Stream<T> active() {
        return members.values().stream().filter(Tracked::isActive).map(Tracked::member);
    }

    @Override
    public int activeCount() {
        return (int) members.values().stream().filter(Tracked::isActive).count();
    }

    @Override
    public List<T> activeMembers() {
        return members.values().stream().filter(Tracked::isActive).map(Tracked::member).toList();
    }

    @Override
    public <Q extends T> void add(Collection<Q> members) {
        members.forEach(this::add);
    }

    @Override
    public boolean add(T m) {
        if (members.containsKey(m.getId())) {
            return false;
        }
        tracking(m);
        return true;
    }

    @Override
    public Stream<T> allMembers() {
        return members.values().stream().map(Tracked::member);
    }

    @Override
    public int cardinality() {
        final var c = cardinality;
        return c;
    }

    @Override
    public void clear() {
        for (Ring<T> ring : rings) {
            ring.clear();
        }
        members.clear();
    }

    @Override
    public void deregister(UUID id) {
        membershipListeners.remove(id);
    }

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built with FF parameters, with the rings
     * forming random graph connections segments.
     */
    @Override
    public int diameter() {
        double pN = ((double) (bias * toleranceLevel())) / ((double) cardinality);
        double logN = Math.log(cardinality);
        return (int) (logN / Math.log(cardinality * pN));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if ((obj == null) || (getClass() != obj.getClass()))
            return false;
        Context<?> other = (Context<?>) obj;
        if (id == null) {
            return other.getId() == null;
        } else
            return id.equals(other.getId());
    }

    @Override
    public T getActiveMember(Digest memberID) {
        var tracked = members.get(memberID);
        return tracked == null ? null : tracked.member();
    }

    @Override
    public List<T> getAllMembers() {
        return allMembers().toList();
    }

    @Override
    public int getBias() {
        return bias;
    }

    @Override
    public double getEpsilon() {
        return epsilon;
    }

    @Override
    public Digest getId() {
        return id;
    }

    @Override
    public T getMember(Digest memberID) {
        var tracked = members.get(memberID);
        return tracked == null ? null : tracked.member();
    }

    @Override
    public Collection<T> getOffline() {
        return members.values().stream().filter(e -> !e.isActive()).map(Tracked::member).toList();
    }

    @Override
    public double getProbabilityByzantine() {
        return pByz;
    }

    @Override
    public short getRingCount() {
        return (short) rings.size();
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public Digest hashFor(T m, int ring) {
        assert ring >= 0 && ring < rings.size() : "Invalid ring: " + ring + " max: " + (rings.size() - 1);
        var tracked = members.get(m.getId());
        if (tracked == null) {
            return hashFor(m.getId(), ring);
        }
        return tracked.hash(ring);
    }

    @Override
    public boolean isActive(Digest id) {
        assert id != null;
        var member = members.get(id);
        if (member == null) {
            return false;
        }
        return member.isActive();
    }

    @Override
    public boolean isActive(T m) {
        assert m != null;
        return isActive(m.getId());
    }

    @Override
    public boolean isMember(Digest digest) {
        return members.containsKey(digest);
    }

    @Override
    public boolean isMember(T m) {
        return members.containsKey(m.getId());
    }

    @Override
    public boolean isOffline(Digest digest) {
        assert digest != null;
        var member = members.get(digest);
        if (member == null) {
            return true;
        }
        return !member.isActive();
    }

    @Override
    public boolean isOffline(T m) {
        assert m != null;
        return isOffline(m.getId());
    }

    @Override
    public boolean isSuccessorOf(T m, Digest digest) {
        for (Ring<T> ring : rings) {
            if (m.equals(ring.successor(m))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int majority(boolean bootstrapped) {
        var majority = getRingCount() - toleranceLevel();
        if (bootstrapped) {
            return switch (totalCount()) {
                case 1, 2 -> 1;
                case 3 -> 2;
                case 4 -> 3;
                default -> majority;
            };
        } else {
            return majority;
        }
    }

    @Override
    public int memberCount() {
        return members.size();
    }

    @Override
    public <Q extends T> void offline(Collection<Q> members) {
        members.forEach(this::offline);
    }

    /**
     * Take a member offline
     */
    @Override
    public boolean offline(T m) {
        if (tracking(m).offline()) {
            membershipListeners.values().forEach(l -> {
                try {
                    l.offline(m);
                } catch (Throwable e) {
                    log.error("error sending fail to listener: " + l, e);
                }
            });
            return true;
        }
        return false;
    }

    @Override
    public int offlineCount() {
        return (int) members.values().stream().filter(e -> !e.isActive()).count();
    }

    /**
     * Take a member offline if already a member
     */
    @Override
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
    @Override
    public List<T> predecessors(Digest key) {
        return predecessors(key, t -> true);
    }

    /**
     * @return the predecessor on each ring for the provided key that pass the provided predicate
     */
    @Override
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
    @Override
    public List<T> predecessors(T key) {
        return predecessors(key, t -> true);
    }

    /**
     * @return the predecessor on each ring for the provided key that pass the provided predicate
     */
    @Override
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

    @Override
    public void rebalance() {
        rebalance(members.size());
    }

    @Override
    public void rebalance(int newCardinality) {
        this.cardinality = Math.max(bias + 1, newCardinality);
        final var ringCount = minMajority(pByz, cardinality, epsilon, bias) * bias + 1;
        members.values().forEach(t -> t.rebalance(ringCount, this));
        final var currentCount = rings.size();
        if (ringCount < currentCount) {
            for (int i = 0; i < currentCount - ringCount; i++) {
                var removed = rings.removeLast();
                removed.clear();
            }
        } else if (ringCount > currentCount) {
            final var added = new ArrayList<Ring<T>>();
            for (int i = currentCount; i < ringCount; i++) {
                final var ring = new Ring<>(i, this);
                rings.add(ring);
                added.add(ring);
            }
            assert rings.size() == ringCount : "Whoops: " + rings.size() + " != " + ringCount;
            members.values().forEach(t -> {
                for (var ring : added) {
                    ring.insert(t.member);
                }
            });
        }
        assert rings.size() == ringCount : "Ring count: " + rings.size() + " does not match: " + ringCount;
        log.debug("Rebalanced: {} from: {} to: {} tolerance: {}", id, currentCount, rings.size(), toleranceLevel());
    }

    @Override
    public UUID register(MembershipListener<T> listener) {
        var uuid = UUID.randomUUID();
        membershipListeners.put(uuid, listener);
        return uuid;
    }

    @Override
    public <Q extends T> void remove(Collection<Q> members) {
        members.forEach(this::remove);
    }

    @Override
    public void remove(Digest id) {
        var removed = members.remove(id);
        if (removed != null) {
            for (Ring<T> ring : rings) {
                ring.delete(removed.member);
            }
        }
    }

    /**
     * remove a member from the receiving DynamicContext
     */
    @Override
    public void remove(T m) {
        remove(m.getId());
    }

    /**
     * @return the indexed Ring<T>
     */
    @Override
    public Ring<T> ring(int index) {
        if (index < 0 || index >= rings.size()) {
            throw new IllegalArgumentException("Not a valid ring #: " + index + " max: " + (rings.size() - 1));
        }
        return rings.get(index);
    }

    /**
     * @return the Stream of rings managed by the context
     */
    @Override
    public Stream<Ring<T>> rings() {
        return rings.stream();
    }

    /**
     * Answer a random sample of at least range size from the active members of the context
     *
     * @param range    - the desired range
     * @param entropy  - source o randomness
     * @param excluded - predicate to test for exclusion
     * @return a random sample set of the view's live members. May be limited by the number of active members.
     */
    @Override
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Predicate<T> excluded) {
        return rings.get(entropy.nextInt(rings.size()))
                    .stream()
                    .collect(new ReservoirSampler<>(excluded, range, entropy));
    }

    /**
     * Answer a random sample of at least range size from the active members of the context
     *
     * @param range   - the desired range
     * @param entropy - source o randomness
     * @param exc     - the member to exclude from sample
     * @return a random sample set of the view's live members. May be limited by the number of active members.
     */
    @Override
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Digest exc) {
        Member excluded = exc == null ? null : getMember(exc);
        return rings.get(entropy.nextInt(rings.size()))
                    .stream()
                    .collect(new ReservoirSampler<T>(excluded, range, entropy));
    }

    @Override
    public int size() {
        return members.size();
    }

    /**
     * @return the list of successors to the key on each ring
     */
    @Override
    public List<T> successors(Digest key) {
        return successors(key, t -> true);
    }

    /**
     * @return the list of successor to the key on each ring that pass the provided predicate test
     */
    @Override
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
    @Override
    public List<T> successors(T key) {
        return successors(key, t -> true);
    }

    /**
     * @return the list of successor to the key on each ring that pass the provided predicate test
     */
    @Override
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
     * The number of iterations until a given message has been distributed to all members in the context, using the
     * rings of the receiver as a gossip graph
     */
    @Override
    public int timeToLive() {
        return (rings.size() * diameter()) + 1;
    }

    @Override
    public String toString() {
        return "DynamicContext [" + id + "]";
    }

    /**
     * Answer the tolerance level of the context to byzantine members, assuming this context has been constructed from
     * FF parameters
     */
    @Override
    public int toleranceLevel() {
        return (rings.size() - 1) / bias;
    }

    @Override
    public int totalCount() {
        return members.size();
    }

    @Override
    public boolean validRing(int ring) {
        return ring >= 0 && ring < rings.size();
    }

    private Digest[] hashesFor(T m) {
        Digest key = m.getId();
        Digest[] s = new Digest[rings.size()];
        for (int ring = 0; ring < rings.size(); ring++) {
            s[ring] = hashFor(key, ring);
        }
        return s;
    }

    private Tracked<T> tracking(T m) {
        return members.computeIfAbsent(m.getId(), id1 -> {
            for (var ring : rings) {
                ring.insert(m);
            }
            return new Tracked<>(m, () -> hashesFor(m));
        });
    }

}
