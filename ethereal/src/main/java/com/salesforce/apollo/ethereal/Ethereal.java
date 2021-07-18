/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.security.PublicKey;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public class Ethereal {
    private final Context<Member> context;
    // members assigned a process id, 0 -> Context cardinality. Determined by id
    // (hash) sort order
    private final SortedMap<Digest, Short>  pids;
    private final PublicKey[]               publicKeys;
    private final PublicKey[]               p2pKeys;
    private final Map<String, List<String>> setupAddresses = new HashMap<>();
    private final Map<String, List<String>> addresses      = new HashMap<>();

    public Ethereal(Config configuration, Context<Member> context) {
        this.context = context;
        AtomicInteger i = new AtomicInteger(0);
        // Organize the members of the context by labeling them with short integer
        // values. The pids maps members to this id, and the map is sorted and ordered
        // by member id, with key index == member's pid
        pids = context.allMembers().sorted()
                      .collect(Collectors.toMap(m -> m.getId(), m -> (short) i.getAndIncrement(), (a, b) -> {
                          throw new IllegalStateException();
                      }, () -> new TreeMap<>()));
        publicKeys = new PublicKey[pids.size()];
        p2pKeys = new PublicKey[pids.size()];
    }
}
