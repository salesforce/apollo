/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.ethereal;

import com.salesfoce.apollo.ethereal.proto.Gossip;
import com.salesfoce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.cryptography.Digest;

/**
 * 
 * @author hal.hildebrand
 *
 */
public interface Processor {

    /**
     * First phase request. Answer the gossip for the current state of the receiver
     * 
     * @param context - the digest id of the context for routing
     * @param ring    - the ring we're gossiping on
     * @return the Gossip
     */
    Gossip gossip(Digest context, int ring);

    /**
     * First phase reply. Answer the Update from the receiver's state, based on the
     * suppled Have
     * 
     * @param gossip - the state contained by the partner
     * @return the Update based on the current state of the receiver and the have
     *         state of the partner
     */
    Update gossip(Gossip gossip);

    /**
     * Second phase, update the receiver state from the supplied update. Return an
     * update based on the current state and the haves of the supplied update
     * 
     * @param update - the Update from the partner
     * @return the Update from the current state of the receiver and the have state
     *         of the supplied update
     */
    Update update(Update update);

    /**
     * Final phase; update the commit, prevote and unit state from the supplied
     * update
     * 
     * @param update - the Update from our partner
     */
    void updateFrom(Update update);
}
