/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common;

import java.util.Set;

import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */

//AcceptedHandler defines how a consensus engine reacts to messages pertaining
//to accepted containers from other validators. Functions only return fatal
//errors if they occur.
public interface AcceptedHandler {

    // Notify this engine of a request to filter non-accepted vertices.
    //
    // This function can be called by any validator. It is not safe to assume
    // this message is utilizing a unique requestID. However, the validatorID is
    // assumed to be authenticated.
    //
    // This engine should respond with an Accepted message with the same
    // requestID, and the subset of the containerIDs that this node has decided
    // are accepted.
    public void getAccepted(ShortID validatorID, int requestID, Set<ID> containerIDs);
    
    // Notify this engine of a set of accepted vertices.
    //
    // This function can be called by any validator. It is not safe to assume
    // this message is in response to a GetAccepted message, is utilizing a
    // unique requestID, or that the containerIDs are a subset of the
    // containerIDs from a GetAccepted message. However, the validatorID is
    // assumed to be authenticated.
    public void accepted(ShortID validatorID, int requestID, Set<ID> containerIDs);
    
    // Notify this engine that a get accepted request it issued has failed.
    //
    // This function will be called if the engine sent a GetAccepted message
    // that is not anticipated to be responded to. This could be because the
    // recipient of the message is unknown or if the message request has timed
    // out.
    //
    // The validatorID, and requestID, are assumed to be the same as those sent
    // in the GetAccepted message.
    public void getAcceptedFailure(ShortID validatorID, int requestID);
}
