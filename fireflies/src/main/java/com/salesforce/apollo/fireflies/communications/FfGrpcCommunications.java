/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.communications;

import java.net.InetSocketAddress;

import com.salesforce.apollo.fireflies.CertWithKey;
import com.salesforce.apollo.fireflies.FirefliesParameters;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.fireflies.View;

/**
 * @author hal.hildebrand
 *
 */
public class FfGrpcCommunications implements FirefliesCommunications {

    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public FfClientCommunications connectTo(Participant to, Node from) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void initialize(View view) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Node newNode(CertWithKey identity, FirefliesParameters parameters) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Node newNode(CertWithKey identity, FirefliesParameters parameters, InetSocketAddress[] boundPorts) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void start() {
        // TODO Auto-generated method stub
        
    }

}
