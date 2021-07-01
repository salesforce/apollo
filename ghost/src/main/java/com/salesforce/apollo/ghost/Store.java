/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import java.security.SecureRandom;
import java.util.List;

import com.salesfoce.apollo.ghost.proto.Binding;
import com.salesfoce.apollo.ghost.proto.Content;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Store {

    public Entries entriesIn(CombinedIntervals combinedIntervals, int maxEntries);

    void add(List<Content> entries);

    void bind(Digest key, Binding binding);

    Content get(Digest cid);

    Binding lookup(Digest key);

    void populate(CombinedIntervals keyIntervals, double fpr, SecureRandom entropy);

    void purge(Digest cid);

    void put(Digest cid, Content content);

    void remove(Digest key);

}
