/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.ghost.communications;

import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.ghost.Ghost;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface GhostCommunications {
    /**
     * Stop the communications. Server endpoint for this process is stopped and not accepting inbound connections.
     */
    void close();

    GhostClientCommunications connect(Member to, Node from);

    /**
     * Initialize the view of the reciever. Used to break circular deps
     * 
     * @param ghost
     */
    void initialize(Ghost ghost);

    /**
     * Start the communications. Server endpoint for this process is initialized and running.
     */
    void start();
}
