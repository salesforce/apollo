/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.memberships;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Grouping {
    public static final String  RING_HASH_ALGORITHM = Conversion.SHA_256;
    private static final String RING_HASH_TEMPLATE  = "%s-gossip-%s";

    private final HashKey                    id;
    private final Map<Member, HashKey[]>     hashes = new HashMap<>();
    private final Ring[]                     rings;
    private final ThreadLocal<MessageDigest> d      = new ThreadLocal<>();

    public Grouping(HashKey id, int r) {
        this.id = id;
        this.rings = new Ring[r];
        for (int i = 0; i < r; i++) {
            rings[i] = new Ring(i, this);
        }
    }

    public void insert(Member m) {
        MessageDigest md = d.get();
        if (md == null) {
            try {
                md = MessageDigest.getInstance(RING_HASH_ALGORITHM);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("No hash algorithm found: " + RING_HASH_ALGORITHM);
            }
            d.set(md);
        }
        for (int ring = 0; ring < rings.length; ring++) {
            md.reset();
            md.update(String.format(RING_HASH_TEMPLATE, id, ring).getBytes());
            HashKey[] s = hashes.computeIfAbsent(m, k -> new HashKey[rings.length]);
            s[ring] = new HashKey(md.digest());
        }
    }

    public HashKey hashFor(Member m, int index) {
        HashKey[] hSet = hashes.get(m);
        if (hSet == null) {
            throw new IllegalArgumentException("Member " + m.getId() + " is not part of this group " + id);
        }
        return hSet[index];
    }

}
