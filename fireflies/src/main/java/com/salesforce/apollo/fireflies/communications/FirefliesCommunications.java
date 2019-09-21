/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.salesforce.apollo.fireflies.CertWithKey;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.View;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface FirefliesCommunications {

    /**
     * Stop the communications. Server endpoint for this process is stopped and not accepting inbound connections.
     */
    void close();

    /**
     * Connect with the member on the given address
     * 
     * @param to
     * @param from
     * @return
     * @throws IOException
     */
    FfClientCommunications connectTo(Member to, Node from);

    /**
     * Initialize the view of the reciever. Used to break circular deps
     * 
     * @param view
     */
    void initialize(View view);

    default void logDiag() {
        // Do nothing
    }

    Node newNode(CertWithKey identity, FirefliesParameters parameters);

    Node newNode(CertWithKey identity, FirefliesParameters parameters, InetSocketAddress[] boundPorts);

    /**
     * Start the communications. Server endpoint for this process is initialized and running.
     */
    void start();
}
