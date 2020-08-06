/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Context {
    public static final ThreadLocal<MessageDigest> DIGEST_CACHE = new ThreadLocal<>();
    public static final String  RING_HASH_ALGORITHM = Conversion.SHA_256;

    private static final String RING_HASH_TEMPLATE  = "%s-%s";
    private final Map<Member, HashKey[]>           hashes      = new HashMap<>();
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
        if (md == null) {
            try {
                md = MessageDigest.getInstance(RING_HASH_ALGORITHM);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("No hash algorithm found: " + RING_HASH_ALGORITHM);
            }
            DIGEST_CACHE.set(md);
        }
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
}
