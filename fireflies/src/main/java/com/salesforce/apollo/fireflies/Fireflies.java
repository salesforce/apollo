/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.fireflies.proto.Digests;
import com.salesfoce.apollo.fireflies.proto.Gossip;
import com.salesfoce.apollo.fireflies.proto.Note;
import com.salesfoce.apollo.fireflies.proto.Update;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface Fireflies extends Link {

    ListenableFuture<Gossip> gossip(Digest context, Note note, int ring, Digests digests);

    int ping(Digest context, int ping);

    void update(Digest context, int ring, Update update);

}
