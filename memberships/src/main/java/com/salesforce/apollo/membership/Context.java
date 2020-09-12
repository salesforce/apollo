/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Provides a Context for Membership. Members may be either active or offline.
 * The Context maintains a number of Rings (can be zero) that this Context
 * provides. Each ring has a unique hash of each individual member, and thus
 * each ring has a diffent ring order of the same membership.
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
            boolean accepted = current.equals(indices.get(0));
            if (accepted) {
                indices.subList(1, indices.size());
            }
            current = current + 1;
            return accepted;
        }
    }

    public static final ThreadLocal<MessageDigest>   DIGEST_CACHE       = ThreadLocal.withInitial(() -> {
                                                                            try {
                                                                                return MessageDigest.getInstance(Context.SHA_256);
                                                                            } catch (NoSuchAlgorithmException e) {
                                                                                throw new IllegalStateException(e);
                                                                            }
                                                                        });
    public static final String                       SHA_256            = "sha-256";
    private static final String                      RING_HASH_TEMPLATE = "%s-%s";
    private final ConcurrentNavigableMap<HashKey, T> active             = new ConcurrentSkipListMap<>();
    private final Map<T, HashKey[]>                  hashes             = new ConcurrentHashMap<>();
    private final HashKey                            id;
    private final ConcurrentHashMap<HashKey, T>      offline            = new ConcurrentHashMap<>();
    private final Ring<T>[]                          rings;

    public Context(HashKey id) {
        this(id, 0);
    }

    @SuppressWarnings("unchecked")
    public Context(HashKey id, int r) {
        this.id = id; 
        this.rings = new Ring[r];
        for (int i = 0; i < r; i++) {
            rings[i] = new Ring<T>(i, this);
        }
    }

    /**
     * Mark a member as active in the context
     */
    public void activate(T m) {
        active.computeIfAbsent(m.getId(), id -> m);
        offline.remove(m.getId());
        hash(m);
        for (Ring<T> ring : rings) {
            ring.insert(m);
        }
    }

    public void add(T m) {
        hash(m);
        offline(m);
    }

    public Collection<T> getActive() {
        return active.values();
    }

    public HashKey getId() {
        return id;
    }

    public Collection<T> getOffline() {
        return offline.values();
    }

    public Ring<T>[] getRings() {
        return Arrays.copyOf(rings, rings.length);
    }

    public boolean isActive(T m) {
        return active.containsKey(m.getId());
    }

    public boolean isOffline(T m) {
        return offline.containsKey(m.getId());
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
    }

    /**
     * @return the predecessor on each ring for the provided key
     */
    public List<T> predecessors(HashKey key) {
        List<T> predecessors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            predecessors.add(ring.predecessor(key));
        }
        return predecessors;
    }

    /**
     * @return the predecessor on each ring for the provided key that pass the
     *         provided predicate
     */
    public List<T> predecessors(HashKey key, Predicate<T> test) {
        List<T> predecessors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            predecessors.add(ring.predecessor(key, test));
        }
        return predecessors;
    }

    /**
     * remove a member from the receiving Context
     */
    public void remove(T m) {
        HashKey[] s = hashes.remove(m);
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
     * @param range   - the desired range
     * @param entropy - source o randomness
     * @return a random sample set of the view's live members. May be limited by the
     *         number of active members.
     */
    public Collection<T> sample(int range, SecureRandom entropy) {
        if (active.size() <= range) {
            return active.values();
        }
        Set<Integer> indices = new HashSet<>(range);
        while (indices.size() < range && indices.size() < active.size()) {
            indices.add(entropy.nextInt(range));
        }
        List<T> sample = new ArrayList<>(range);
        Counter index = new Counter(indices);
        active.forEach((uuid, m) -> {
            if (index.accept()) {
                sample.add(m);
            }
        });
        return sample;
    }

    /**
     * @return the list of successors to the key on each ring
     */
    public List<T> successors(HashKey key) {
        List<T> successors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            successors.add(ring.successor(key));
        }
        return successors;
    }

    /**
     * @return the list of successor to the key on each ring that pass the provided
     *         predicate test
     */
    public List<T> successors(HashKey key, Predicate<T> test) {
        List<T> successors = new ArrayList<>();
        for (Ring<T> ring : rings) {
            successors.add(ring.successor(key, test));
        }
        return successors;
    }

    protected HashKey hashFor(T m, int index) {
        HashKey[] hSet = hashes.get(m);
        if (hSet == null) {
            throw new IllegalArgumentException("T " + m.getId() + " is not part of this group " + id);
        }
        return hSet[index];
    }

    private void hash(T m) {
        if (hashes.containsKey(m)) {
            return;
        }
        MessageDigest md = DIGEST_CACHE.get();
        for (int ring = 0; ring < rings.length; ring++) {
            md.reset();
            md.update(String.format(RING_HASH_TEMPLATE, m.getId(), ring).getBytes());
            HashKey[] s = hashes.computeIfAbsent(m, k -> new HashKey[rings.length]);
            s[ring] = new HashKey(md.digest());
        }
    }
}
