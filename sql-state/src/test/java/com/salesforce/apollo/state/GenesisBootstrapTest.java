/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import ch.qos.logback.classic.Level;
import com.chiralbehaviors.tron.Fsm;
import com.salesforce.apollo.choam.*;
import com.salesforce.apollo.context.DynamicContext;
import com.salesforce.apollo.utils.Utils;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author hal.hildebrand
 */
public class GenesisBootstrapTest extends AbstractLifecycleTest {

    static {
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
        ((DynamicContext) choam.context().delegate()).activate(testSubject);
        choam.start();
        routers.get(testSubject.getId()).start();

        assertTrue(Utils.waitForCondition(30_000, 1_000, () -> choam.active()),
                   "Test subject did not become active: " + choam.logState());
        members.add(testSubject);
        post();
    }

    @Override
    protected int checkpointBlockSize() {
        return 1000;
    }

    @Override
    protected byte disc() {
        return 2;
    }
}
