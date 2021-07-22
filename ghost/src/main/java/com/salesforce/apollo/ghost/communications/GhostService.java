/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost.communications;

import com.salesfoce.apollo.ghost.proto.Bind;
import com.salesfoce.apollo.ghost.proto.Binding;
import com.salesfoce.apollo.ghost.proto.ClockMongering;
import com.salesfoce.apollo.ghost.proto.Content;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Entry;
import com.salesfoce.apollo.ghost.proto.Get;
import com.salesfoce.apollo.ghost.proto.GhostChat;
import com.salesfoce.apollo.ghost.proto.Intervals;
import com.salesfoce.apollo.ghost.proto.Lookup;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface GhostService {

    Sig bind(Bind bind, Digest from);

    Content get(Get get, Digest from);

    ClockMongering ghosting(GhostChat chatter, Digest from);

    Entries intervals(Intervals request, Digest from);

    Binding lookup(Lookup query, Digest from);

    void purge(Get get, Digest from);

    Sig put(Entry entry, Digest from);

    void remove(Lookup query, Digest from);

}
