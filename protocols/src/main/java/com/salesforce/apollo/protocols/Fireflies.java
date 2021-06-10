/*
0 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.proto.Digests;
import com.salesfoce.apollo.proto.Gossip;
import com.salesfoce.apollo.proto.Signed;
import com.salesfoce.apollo.proto.Update;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Fireflies {

    ListenableFuture<Gossip> gossip(Digest id, Signed note, int ring, Digests gossip);

    int ping(Digest id, int ping);

    void update(Digest id, int ring, Update update);

}
