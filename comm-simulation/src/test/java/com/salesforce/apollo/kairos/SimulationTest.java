/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.kairos;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

/**
 * @author hal.hildebrand
 *
 */
public class SimulationTest {

    @Test
    public void coroutines() throws Exception {
        final var count = 100;
        var sim = new Simulation();
        var countdown = new CountDownLatch(count);
        var expected = new HashSet<Integer>();
        for (int i = 0; i < count; i++) {
            expected.add(i);
        }

        var result = Collections.newSetFromMap(new ConcurrentHashMap<>());

        var coroutines = new ArrayList<Runnable>();

        for (int i = 0; i < count; i++) {
            final var index = i;
            var futureSailor = new KairosFuture<Void>(() -> countdown.countDown(), sim);
            coroutines.add(() -> {
                try {
                    futureSailor.get();
                    result.add(index);
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                countdown.countDown();
            });
            sim.schedule(Duration.ofNanos(1), futureSailor);
        }

        var exec = Executors.newFixedThreadPool(5);
        for (var r : coroutines) {
            exec.execute(r);
        }

        Thread.sleep(100);

        assertEquals(100, countdown.getCount());

        while (sim.advance()) {
        }

        assertTrue(countdown.await(10, TimeUnit.SECONDS));

        assertEquals(0, Sets.difference(expected, result).size());
    }

    @Test
    public void simultaneousScheduling() throws Exception {
        final var count = 100;
        var result = new StringBuffer();
        var sim = new Simulation();
        var countdown = new CountDownLatch(count);
        var expected = new StringBuffer();
        for (int i = 0; i < count; i++) {
            expected.append((i == 0 ? "" : ", ") + i);
        }

        for (int i = 0; i < count; i++) {
            final var index = i;
            sim.getScheduler().execute(() -> {
                result.append((index == 0 ? "" : ", ") + index);
                countdown.countDown();
            });
        }

        while (sim.advance()) {
        }

        assertTrue(countdown.await(10, TimeUnit.SECONDS));

        assertEquals(expected.toString(), result.toString());
    }
}
