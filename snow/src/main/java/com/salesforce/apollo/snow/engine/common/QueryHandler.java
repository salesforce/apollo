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
//QueryHandler defines how a consensus engine reacts to query messages from
//other validators.
public interface QueryHandler {
    // Notify this engine of a request for our preferences.
    //
    // This function can be called by any validator. It is not safe to assume
    // this message is utilizing a unique requestID. However, the validatorID is
    // assumed to be authenticated.
    //
    // If the container or its ancestry is incomplete, this engine is expected
    // to request the missing containers from the validator. Once the ancestry
    // is complete, this engine should send this validator the current
    // preferences in a Chits message. The Chits message should have the same
    // requestID that was passed in here.
    void pullQuery(ShortID validatorID, int requestID, ID containerID);
    // Notify this engine of a request for our preferences.
    //
    // This function can be called by any validator. It is not safe to assume
    // this message is utilizing a unique requestID or even that the containerID
    // matches the ID of the container bytes. However, the validatorID is
    // assumed to be authenticated.
    //
    // This function is meant to behave the same way as PullQuery, except the
    // container is optimistically provided to potentially remove the need for
    // a series of Get/Put messages.
    //
    // If the ancestry of the container is incomplete, this engine is expected
    // to request the ancestry from the validator. Once the ancestry is
    // complete, this engine should send this validator the current preferences
    // in a Chits message. The Chits message should have the same requestID that
    // was passed in here.
    void pushQuery(ShortID validatorID, int requestID, ID containerID, byte[] container);
    // Notify this engine of the specified validators preferences.
    //
    // This function can be called by any validator. It is not safe to assume
    // this message is in response to a PullQuery or a PushQuery message.
    // However, the validatorID is assumed to be authenticated.
    void chits(ShortID validatorID, int requestID, Set<ID> containerIDs);

    void queryFailed(ShortID validatorID, int requestId);

}
