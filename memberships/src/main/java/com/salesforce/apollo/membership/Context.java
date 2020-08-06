/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author hal.hildebrand
 *
 */
public class Context {
    public static final ThreadLocal<MessageDigest> DIGEST_CACHE       = ThreadLocal.withInitial(() -> {
                                                                          try {
                                                                              return MessageDigest.getInstance(Context.SHA_256);
                                                                          } catch (NoSuchAlgorithmException e) {
                                                                              throw new IllegalStateException(e);
                                                                          }
                                                                      });
    public static final String                     SHA_256            = "sha-256";
    private static final String                    RING_HASH_TEMPLATE = "%s-%s";
    private final Map<Member, HashKey[]>           hashes             = new HashMap<>();
    private final HashKey                          id;
    private final Ring[]                           rings;

    public Context(HashKey id, int r) {
        this.id = id;
        this.rings = new Ring[r];
        for (int i = 0; i < r; i++) {
            rings[i] = new Ring(i, this);
        }
    }

    public HashKey hashFor(Member m, int index) {
        HashKey[] hSet = hashes.get(m);
        if (hSet == null) {
            throw new IllegalArgumentException("Member " + m.getId() + " is not part of this group " + id);
        }
        return hSet[index];
    }

    public void insert(Member m) {
        MessageDigest md = DIGEST_CACHE.get();
        for (int ring = 0; ring < rings.length; ring++) {
            md.reset();
            md.update(String.format(RING_HASH_TEMPLATE, m.getId(), ring).getBytes());
            HashKey[] s = hashes.computeIfAbsent(m, k -> new HashKey[rings.length]);
            s[ring] = new HashKey(md.digest());
        }
        for (Ring ring : rings) {
            ring.insert(m);
        }
    }

    public List<Member> predecessors(HashKey key) {
        List<Member> predecessors = new ArrayList<>();
        for (Ring ring : rings) {
            predecessors.add(ring.predecessor(key));
        }
        return predecessors;
    }

    public List<Member> predecessors(HashKey key, Predicate<Member> test) {
        List<Member> predecessors = new ArrayList<>();
        for (Ring ring : rings) {
            predecessors.add(ring.predecessor(key, test));
        }
        return predecessors;
    }

    public void remove(Member m) {
        HashKey[] s = hashes.get(m);
        if (s == null) {
            return;
        }
        for (int i = 0; i < s.length; i++) {
            rings[i].delete(m);
        }
    }

    public Stream<Ring> rings() {
        return Arrays.asList(rings).stream();
    }

    public List<Member> successors(HashKey key) {
        List<Member> successors = new ArrayList<>();
        for (Ring ring : rings) {
            successors.add(ring.successor(key));
        }
        return successors;
    }

    public List<Member> successors(HashKey key, Predicate<Member> test) {
        List<Member> successors = new ArrayList<>();
        for (Ring ring : rings) {
            successors.add(ring.successor(key, test));
        }
        return successors;
    }
}
