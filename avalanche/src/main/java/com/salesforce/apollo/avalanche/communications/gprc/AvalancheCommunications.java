/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications.gprc;

import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface AvalancheCommunications {
    /**
     * Stop the communications. Server endpoint for this process is stopped and not accepting inbound connections.
     */
    void close();

    /**
     * Connect ye to ye member
     * 
     * @param to
     * @param from
     * @return comm or null (go home)
     */
    AvalancheClientCommunications connectToNode(Member to, Node from);

    /**
     * Initialize the view of the reciever. Used to break circular deps
     * 
     * @param avalanche
     */
    void initialize(Avalanche avalanche);

    /**
     * Start the communications. Server endpoint for this process is initialized and running.
     */
    void start();
}
