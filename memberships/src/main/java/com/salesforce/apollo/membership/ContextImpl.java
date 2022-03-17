/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.membership.Context.minMajority;

import java.util.ArrayList;
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
public class ContextImpl<T extends Member> implements Context<T> {

    private static final Logger log                = LoggerFactory.getLogger(Context.class);
    private static final String RING_HASH_TEMPLATE = "%s-%s-%s";

    private final int                              bias;
    private volatile int                           cardinality;
    private final Digest                           id;
    private final Map<Digest, Tracked<T>>          members             = new ConcurrentHashMap<>();
    private final Map<UUID, MembershipListener<T>> membershipListeners = new ConcurrentHashMap<>();
    private final double                           pByz;
    private final List<Ring<T>>                    rings               = new ArrayList<>();

    public ContextImpl(int cardinality, double pbyz, int bias, Digest id) {
        this.pByz = pbyz;
        this.id = id;
        this.bias = bias;
        this.cardinality = cardinality;
        for (int i = 0; i < minMajority(pByz, cardinality, 0.99, bias) * bias + 1; i++) {
            rings.add(new Ring<T>(i, (m, ring) -> hashFor(m, ring)));
        }
    }

    @Override
    public void activate(Collection<T> activeMembers) {
        activeMembers.forEach(m -> activate(m));
    }

    /**
     * Mark a member as active in the context
     */
    @Override
    public boolean activate(T m) {
        if (tracking(m).activate()) {
            membershipListeners.values().stream().forEach(l -> {
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
     * Mark a member as active in the context
     */
    @Override
    public boolean activateIfMember(T m) {
        var member = members.get(m.getId());
        if (member != null && member.activate()) {
            membershipListeners.values().stream().forEach(l -> {
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

    @Override
    public int activeCount() {
        return (int) members.values().stream().filter(e -> e.isActive()).count();
    }

    @Override
    public List<T> activeMembers() {
        return members.values().stream().filter(e -> e.isActive()).map(e -> (T) e.member()).toList();
    }

    @Override
    public void add(Collection<T> members) {
        members.forEach(m -> add(m));
    }

    @Override
    public void add(T m) {
        offline(m);
    }

    @Override
    public Stream<T> allMembers() {
        return members.values().stream().map(e -> e.member());
    }

    @Override
    public int cardinality() {
        return cardinality;
    }

    @Override
    public void clear() {
        for (Ring<T> ring : rings) {
            ring.clear();
        }
        members.clear();
    }

    @Override
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

    @Override
    public void deregister(UUID id) {
        membershipListeners.remove(id);
    }

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built
     * with FF parameters, with the rings forming random graph connections segments.
     */
    @Override
    public int diameter() {
        return diameter(size());
    }

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built
     * with FF parameters, with the rings forming random graph connections segments
     * with the supplied cardinality
     */
    @Override
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
            if (other.getId() != null)
                return false;
        } else if (!id.equals(other.getId()))
            return false;
        return true;
    }

    @Override
    public Collection<T> getActive() {
        return activeMembers();
    }

    @Override
    public T getActiveMember(Digest memberID) {
        var tracked = members.get(memberID);
        return tracked == null ? null : tracked.member();
    }

    @Override
    public int getBias() {
        return bias;
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
        return members.values().stream().filter(e -> !e.isActive()).map(e -> e.member()).toList();
    }

    @Override
    public double getProbabilityByzantine() {
        return pByz;
    }

    @Override
    public int getRingCount() {
        return rings.size();
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public Digest hashFor(T m, int index) {
        var tracked = members.get(m.getId());
        if (tracked == null) {
            log.debug("{} is not part of this group: {} on: {} ", m, id);
            return null;
        }
        return tracked.hash(index);
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
    public int majority() {
        return getRingCount() - toleranceLevel();
    }

    @Override
    public int memberCount() {
        return members.size();
    }

    @Override
    public void offline(Collection<T> members) {
        members.forEach(m -> offline(m));
    }

    /**
     * Take a member offline
     */
    @Override
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
     * @return the predecessor on each ring for the provided key that pass the
     *         provided predicate
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
     * @return the predecessor on each ring for the provided key that pass the
     *         provided predicate
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
    public UUID register(MembershipListener<T> listener) {
        var uuid = UUID.randomUUID();
        membershipListeners.put(uuid, listener);
        return uuid;
    }

    @Override
    public void remove(Collection<T> members) {
        members.forEach(m -> remove(m));
    }

    /**
     * remove a member from the receiving Context
     */
    @Override
    public void remove(T m) {
        members.remove(m.getId());
        for (int i = 0; i < rings.size(); i++) {
            rings.get(i).delete(m);
        }
    }

    /**
     * @return the indexed Ring<T>
     */
    @Override
    public Ring<T> ring(int index) {
        if (index < 0 || index >= rings.size()) {
            throw new IllegalArgumentException("Not a valid ring #: " + index);
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
     * Answer a random sample of at least range size from the active members of the
     * context
     * 
     * @param range    - the desired range
     * @param entropy  - source o randomness
     * @param excluded - the member to exclude from sample
     * @return a random sample set of the view's live members. May be limited by the
     *         number of active members.
     */
    @Override
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Digest exc) {
        Member excluded = getMember(exc);
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
     * @return the list of successor to the key on each ring that pass the provided
     *         predicate test
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
     * @return the list of successor to the key on each ring that pass the provided
     *         predicate test
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
     * The number of iterations until a given message has been distributed to all
     * members in the context, using the rings of the receiver as a gossip graph
     */
    @Override
    public int timeToLive() {
        return (rings.size() * diameter()) + 1;
    }

    /**
     * Answer the tolerance level of the context to byzantine members, assuming this
     * context has been constructed from FF parameters
     */
    @Override
    public int toleranceLevel() {
        return (rings.size() - 1) / 2;
    }

    @Override
    public String toString() {
        return "Context [" + id + "]";
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
            return new Tracked<>(m, hashesFor(m));
        });
        if (added.get()) {
            for (var ring : rings) {
                ring.insert(m);
            }
        }
        return member;
    }
}
