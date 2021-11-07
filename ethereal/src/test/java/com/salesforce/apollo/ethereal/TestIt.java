/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

/**
 * @author hal.hildebrand
 *
 */
public class TestIt {

    @Test
    public void test() throws Exception {
        final var count = 100000;
        CountDownLatch countdown = new CountDownLatch(count);
        ExecutorService executor = Executors.newFixedThreadPool(1, r -> new Thread(r, "Test"));
        for (int i = 0; i < count; i++) {
            var c= i;
            executor.execute(() -> {
                System.out.println("i:" + c);
                countdown.countDown();
            });
        }
        assertTrue(countdown.await(300, TimeUnit.SECONDS));
        executor.shutdown();
        assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));
        System.out.println("Ready");
        Thread.sleep(300_000);
    }
}
