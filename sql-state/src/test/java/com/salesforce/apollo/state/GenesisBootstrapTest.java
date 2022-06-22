/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Fsm;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.Committee;
import com.salesforce.apollo.choam.GenesisAssembly;
import com.salesforce.apollo.choam.Producer;
import com.salesforce.apollo.choam.Session;
import com.salesforce.apollo.choam.ViewAssembly;

import ch.qos.logback.classic.Level;

/**
 * @author hal.hildebrand
 *
 */
public class GenesisBootstrapTest extends AbstractLifecycleTest {
    static {
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Session.class)).setLevel(Level.TRACE);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CHOAM.class)).setLevel(Level.TRACE);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(GenesisAssembly.class)).setLevel(Level.TRACE);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ViewAssembly.class)).setLevel(Level.TRACE);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Producer.class)).setLevel(Level.TRACE);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Committee.class)).setLevel(Level.TRACE);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Fsm.class)).setLevel(Level.TRACE);
    }

    @Test
    public void genesisBootstrap() throws Exception {
        pre();
        System.out.println("Starting late joining node");
        var choam = choams.get(testSubject.getId());
        choam.context().activate(Collections.singletonList(testSubject));
        choam.start();
        routers.get(testSubject.getId()).start();
        post();
    }

    @Override
    protected int checkpointBlockSize() {
        return 10;
    }
}
