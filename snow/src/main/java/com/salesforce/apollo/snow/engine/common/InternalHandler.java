/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common;

import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
//InternalHandler defines how this consensus engine reacts to messages from
//other components of this validator. Functions only return fatal errors if
//they occur.
public interface InternalHandler {
    // Startup this engine.
    //
    // This function will be called once the environment is configured to be
    // able to run the engine.
    void startUp();

    // Gossip to the network a container on the accepted frontier
    void gossip();

    // Shutdown this engine.
    //
    // This function will be called when the environment is exiting.
    void shutdown();

    // Notify this engine of a message from the virtual machine.
    void Notify();

    // Notify this engine of a new peer.
    void connectected(ShortID validatorId);

    // Notify this engine of a removed peer.
    void disconnected(ShortID validatorId);

}
