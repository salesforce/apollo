/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;

/**
 * Provides a Context for Membership and is uniquely identified by a Digest;.
 * Members may be either active or offline. The Context maintains a number of
 * Rings (may be zero) that the Context provides for Firefly type ordering
 * operators. Each ring has a unique hash of each individual member, and thus
 * each ring has a different ring order of the same membership set. Hashes for
 * Context level operators include the ID of the ring. Hashes computed for each
 * member, per ring include the ID of the enclosing Context.
 * 
 * @author hal.hildebrand
 *
 */
public class Context<T extends Member> {

    public static class Counter {
        private Integer       current = 0;
        private List<Integer> indices;

        public Counter(Set<Integer> indices) {
            this.indices = new ArrayList<>(indices);
            Collections.sort(this.indices);
        }

        public boolean accept() {
            boolean accepted = indices.isEmpty() ? false : current.equals(indices.get(0));
            if (accepted) {
                indices = indices.subList(1, indices.size());
            }
            current = current + 1;
            return accepted;
        }
    }

    public interface MembershipListener<T> {

        /**
         * A member has failed
         * 
         * @param member
         */
        default void fail(Member member) {
        };

        /**
         * A new member has recovered and is now live
         * 
         * @param member
         */
        default void recover(Member member) {
        };
    }

    public static final ThreadLocal<MessageDigest> DIGEST_CACHE = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance(Context.SHA_256);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    });

    public static final String SHA_256 = "sha-256";

    private static final String RING_HASH_TEMPLATE = "%s-%s-%s";

    /**
     * @return the minimum t such that the probability of more than t out of 2t+1
     *         monitors are correct with probability e/size given the uniform
     *         probability pByz that a monitor is Byzantine.
     */
    public static int minMajority(double pByz, int cardinality) {
        return minMajority(pByz, cardinality, 0.99);
    }

    /**
     * @return the minimum t such that the probability of more than t out of 2t+1
     *         monitors are correct with probability e/size given the uniform
     *         probability pByz that a monitor is Byzantine.
     */
    public static int minMajority(double pByz, int cardinality, double epsilon) {
        if (epsilon > 1.0 || epsilon <= 0.0) {
            throw new IllegalArgumentException("epsilon must be > 0 and <= 1 : " + epsilon);
        }
        double e = epsilon / cardinality;
        for (int t = 1; t <= cardinality; t++) {
            double pf = 1.0 - Util.binomialc(t, 2 * t + 1, pByz);
            if (e >= pf) {
                if (cardinality >= (2 * t) + 1) {
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

    private final Map<Digest, T>               active              = new ConcurrentHashMap<>();
    private final Map<Digest, Digest[]>        hashes              = new ConcurrentHashMap<>();
    private final Digest                       id;
    private Logger                             log                 = LoggerFactory.getLogger(Context.class);
    private final List<MembershipListener<T>>  membershipListeners = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<Digest, T> offline             = new ConcurrentHashMap<>();
    private final Ring<T>[]                    rings;

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
        this(id, minMajority(pByz, cardinality) * 2 + 1);
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
    public Context(Digest id, double pByz, int cardinality, double epsilon) {
        this(id, minMajority(pByz, cardinality, epsilon) * 2 + 1);
    }

    @SuppressWarnings("unchecked")
    public Context(Digest id, int r) {
        this.id = id;
        this.rings = new Ring[r];
        for (int i = 0; i < r; i++) {
            rings[i] = new Ring<T>(i, (m, ring) -> hashFor(m, ring));
        }
    }

    /**
     * Mark a member as active in the context
     */
    public void activate(T m) {
        active.computeIfAbsent(m.getId(), id -> m);
        offline.remove(m.getId());
        for (Ring<T> ring : rings) {
            ring.insert(m);
        }
        membershipListeners.stream().forEach(l -> {
            try {
                l.recover(m);
            } catch (Throwable e) {
                log.error("error recoving member in listener: " + l, e);
            }
        });
    }

    public boolean activateIfOffline(Digest memberID) {
        T offlined = offline.remove(memberID);
        if (offlined == null) {
            return false;
        }
        activate(offlined);
        return true;
    }

    public List<T> activeMembers() {
        return new ArrayList<>(active.values());
    }

    public void add(T m) {
        offline(m);
    }

    public Stream<T> allMembers() {
        return Arrays.asList(active.values(), offline.values()).stream().flatMap(c -> c.stream());
    }

    public int cardinality() {
        return active.size() + offline.size();
    }

    public void clear() {
        for (Ring<T> ring : rings) {
            ring.clear();
        }
        hashes.clear();
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
        return active.values();
    }

    public T getActiveMember(Digest memberID) {
        return active.get(memberID);
    }

    public Digest getId() {
        return id;
    }

    public T getMember(Digest memberID) {
        T member = active.get(memberID);
        if (member == null) {
            member = offline.get(memberID);
        }
        return member;
    }

    public Collection<T> getOffline() {
        return offline.values();
    }

    public int getRingCount() {
        return rings.length;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public boolean isActive(T m) {
        assert m != null;
        return active.containsKey(m.getId());
    }

    public boolean isOffline(Digest digest) {
        return offline.containsKey(digest);
    }

    public boolean isOffline(T m) {
        return offline.containsKey(m.getId());
    }

    public int majority() {
        return getRingCount() - toleranceLevel();
    }

    /**
     * Take a member offline
     */
    public void offline(T m) {
        active.remove(m.getId());
        offline.computeIfAbsent(m.getId(), id -> m);
        for (Ring<T> ring : rings) {
            ring.delete(m);
        }
        membershipListeners.stream().forEach(l -> {
            try {
                l.fail(m);
            } catch (Throwable e) {
                log.error("error sending fail to listener: " + l, e);
            }
        });
    }

    public boolean offlineIfActive(Digest memberID) {
        T activated = active.remove(memberID);
        if (activated == null) {
            return false;
        }
        offline(activated);
        return true;
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

    public void register(MembershipListener<T> listener) {
        membershipListeners.add(listener);
    }

    /**
     * remove a member from the receiving Context
     */
    public void remove(T m) {
        Digest[] s = hashes.remove(m.getId());
        if (s == null) {
            return;
        }
        active.remove(m.getId());
        offline.remove(m.getId());
        for (int i = 0; i < s.length; i++) {
            rings[i].delete(m);
        }
    }

    /**
     * @return the indexed Ring<T>
     */
    public Ring<T> ring(int index) {
        if (index < 0 || index >= rings.length) {
            throw new IllegalArgumentException("Not a valid ring #: " + index);
        }
        return rings[index];
    }

    /**
     * @return the Stream of rings managed by the context
     */
    public Stream<Ring<T>> rings() {
        return Arrays.asList(rings).stream();
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
        return rings[entropy.nextInt(rings.length)].stream().collect(new ReservoirSampler<T>(excluded, range, entropy));
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
        return (rings.length * diameter()) + 1;
    }

    /**
     * Answer the tolerance level of the context to byzantine members, assuming this
     * context has been constructed from FF parameters
     */
    public int toleranceLevel() {
        return (rings.length - 1) / 2;
    }

    @Override
    public String toString() {
        return "Context [id=" + id + " " + ring(0) + "]";
    }

    Digest hashFor(T m, int index) {
        Digest[] hSet = hashes.computeIfAbsent(m.getId(), k -> {
            Digest[] s = new Digest[rings.length];
            for (int ring = 0; ring < rings.length; ring++) {
                Digest key = m.getId();
                s[ring] = key.getAlgorithm()
                             .digest(String.format(RING_HASH_TEMPLATE, qb64(id), qb64(key), ring).getBytes());
            }
            return s;
        });
        if (hSet == null) {
            throw new IllegalArgumentException(m + " is not part of this group " + id);
        }
        return hSet[index];
    }
}
