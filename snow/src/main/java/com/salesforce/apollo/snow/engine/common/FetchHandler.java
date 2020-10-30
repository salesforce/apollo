/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common;

import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
//FetchHandler defines how a consensus engine reacts to retrieval messages from
//other validators.
public interface FetchHandler {
    // Notify this engine of a request for a container.
    //
    // This function can be called by any validator. It is not safe to assume
    // this message is utilizing a unique requestID. It is also not safe to
    // assume the requested containerID exists. However, the validatorID is
    // assumed to be authenticated.
    //
    // There should never be a situation where a virtuous node sends a Get
    // request to another virtuous node that does not have the requested
    // container. Unless that container was pruned from the active set.
    //
    // This engine should respond with a Put message with the same requestID if
    // the container was locally avaliable. Otherwise, the message can be safely
    // dropped.
    void get(ShortID validatorId, int requestId, ID containerID);

    // Notify this engine of a request for a container and its ancestors.
    // The request is from validator [validatorID]. The requested container is
    // [containerID].
    //
    // This function can be called by any validator. It is not safe to assume
    // this message is utilizing a unique requestID. It is also not safe to
    // assume the requested containerID exists. However, the validatorID is
    // assumed to be authenticated.
    //
    // This engine should respond with a MultiPut message with the same requestID,
    // which contains [containerID] as well as its ancestors. See MultiPut's
    // documentation.
    //
    // If this engine doesn't have some ancestors, it should reply with its best
    // effort attempt at getting them.
    // If this engine doesn't have [containerID] it can ignore this message.
    void getAncestors(ShortID validatorId, int requestID, ID containerID);

    // Notify this engine of a container.
    //
    // This function can be called by any validator. It is not safe to assume
    // this message is utilizing a unique requestID or even that the containerID
    // matches the ID of the container bytes. However, the validatorID is
    // assumed to be authenticated.
    //
    // This engine needs to request and receive missing ancestors of the
    // container before adding the container to consensus. Once all ancestor
    // containers are added, pushes the container into the consensus.
    void put(ShortID validatorID, int requestID, ID containerID, byte[] container);

    // Notify this engine of multiple containers.
    // Each element of [containers] is the byte representation of a container.
    //
    // This should only be called during bootstrapping, and in response to a
    // GetAncestors message to
    // [validatorID] with request ID [requestID]. This call should contain the
    // container requested in
    // that message, along with ancestors.
    // The containers should be in BFS order (ie the first container must be the
    // container
    // requested in the GetAncestors message and further back ancestors are later in
    // [containers]
    //
    // It is not safe to assume this message is in response to a GetAncestor
    // message, that this
    // message has a unique requestID or that any of the containers in [containers]
    // are valid.
    // However, the validatorID is assumed to be authenticated.
    void multiPut(ShortID validatorID, int requestId, byte[][] containers);

    // Notify this engine that a get request it issued has failed.
    //
    // This function will be called if the engine sent a Get message that is not
    // anticipated to be responded to. This could be because the recipient of
    // the message is unknown or if the message request has timed out.
    //
    // The validatorID and requestID are assumed to be the same as those sent in
    // the Get message.
    void getFailed(ShortID validatorID, int requestID);

    // Notify this engine that a GetAncestors request it issued has failed.
    //
    // This function will be called if the engine sent a GetAncestors message that
    // is not
    // anticipated to be responded to. This could be because the recipient of
    // the message is unknown or if the message request has timed out.
    //
    // The validatorID and requestID are assumed to be the same as those sent in
    // the GetAncestors message.
    void getAncestorsFailed(ShortID validatorID, int requestID);
}
