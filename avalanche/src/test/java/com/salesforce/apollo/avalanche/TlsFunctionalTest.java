/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import org.junit.Before;

import com.salesforce.apollo.avalanche.communications.AvalancheCommunications;
import com.salesforce.apollo.avalanche.communications.netty.AvalancheNettyCommunications;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * @author hhildebrand
 *
 */
public class TlsFunctionalTest extends AvalancheFunctionalTest {

    private EventLoopGroup     eventLoop;
    private EventExecutorGroup executor;

    @Before
    public void beforeTest() {
        eventLoop = new NioEventLoopGroup(20);
        executor = new DefaultEventExecutorGroup(30);
    }

    protected AvalancheCommunications getCommunications() {
        return new AvalancheNettyCommunications(rpcStats, eventLoop, eventLoop, eventLoop, executor, executor);
    }

}
