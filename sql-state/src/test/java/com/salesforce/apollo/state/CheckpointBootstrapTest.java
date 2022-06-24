/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.joou.ULong;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointBootstrapTest extends AbstractLifecycleTest {

    static {
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Session.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(CHOAM.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(GenesisAssembly.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ViewAssembly.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Producer.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Committee.class)).setLevel(Level.TRACE);
//        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(Fsm.class)).setLevel(Level.TRACE);
    }

    @Test
    public void checkpointBootstrap() throws Exception {
        pre();

        checkpointOccurred.await(30, TimeUnit.SECONDS);

        ULong chkptHeight = checkpointHeight.get();
        assertNotNull(chkptHeight, "Null checkpoint height!");
        System.out.println("Checkpoint at height: " + chkptHeight);

        assertTrue(Utils.waitForCondition(10_000, 1_000, () -> {
            return members.stream()
                          .filter(m -> !m.equals(testSubject))
                          .map(m -> updaters.get(m))
                          .map(ssm -> ssm.getCurrentBlock())
                          .filter(cb -> cb != null)
                          .map(cb -> cb.height())
                          .filter(l -> l.compareTo(chkptHeight) >= 0)
                          .count() == members.size() - 1;
        }), "All members did not process the checkpoint");

        System.out.println("Starting late joining node");
        var choam = choams.get(testSubject.getId());
        choam.context().activate(testSubject);
        choam.start();
        routers.get(testSubject.getId()).start();

        assertTrue(Utils.waitForCondition(30_000, 1_000, () -> choam.active()), "Test subject did not become active");

        post();
    }

    @Override
    protected int checkpointBlockSize() {
        return 1;
    }
}
