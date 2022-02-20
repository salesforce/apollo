/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class TestGossiper {
    private ScheduledFuture<?>             futureSailor;
    private final List<GossipService>      gossipers;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean            started = new AtomicBoolean();
    private final AtomicInteger            round   = new AtomicInteger();

    public TestGossiper(List<Orderer> orderers) {
        scheduler = Executors.newScheduledThreadPool(50);
        gossipers = orderers.stream().map(o -> new GossipService(o)).collect(Collectors.toList());
        Collections.shuffle(gossipers);
    }

    public void close() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        if (futureSailor != null) {
            futureSailor.cancel(true);
        }
    }

    public void start(Duration duration) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        var millis = duration.toMillis();
        futureSailor = scheduler.scheduleWithFixedDelay(() -> gossip(), Utils.bitStreamEntropy().nextLong(2 * millis),
                                                        millis, TimeUnit.MILLISECONDS);
    }

    // Execute one round of gossip
    private void gossip() {
        final var thisRound = round.getAndIncrement() % gossipers.size();
        try {
            gossipers.parallelStream().forEach(g -> {
                try {
                    Thread.sleep(Utils.bitStreamEntropy().nextInt(5));
                } catch (InterruptedException e) {
                }
                var candidate = gossipers.get(thisRound);
                if (candidate != g) {
                    g.update(candidate.gossip(g.gossip(DigestAlgorithm.DEFAULT.getOrigin(), 0)));
                }
            });
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
