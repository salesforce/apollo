/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 222
 */
public class Transactioneer {
    private final AtomicInteger counter = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private volatile ScheduledFuture<?> futureSailor;
    private final Avalanche node;
    private final List<CompletableFuture<HashKey>> outstanding = new CopyOnWriteArrayList<>();
    private final AtomicInteger success = new AtomicInteger();

    public Transactioneer(Avalanche node) {
        this.node = node;
    }

    public int getFailed() {
        return failed.get();
    }

    public UUID getId() {
        return node.getNode().getId();
    }

    public int getSuccess() {
        return success.get();
    }

    public void stop() {
        ScheduledFuture<?> current = futureSailor;
        futureSailor = null;
        if (current != null) {
            current.cancel(true);
        }
    }

    @Override
    public String toString() {
        return getId() + " s: " + getSuccess() + " f: " + getFailed();
    }

    public void transact(Duration txnWait, int maintain, ScheduledExecutorService scheduler) {
        scheduler.scheduleWithFixedDelay(() -> {
            if (outstanding.size() < maintain) {
                addTransaction(txnWait, scheduler);
                addTransaction(txnWait, scheduler);
            }
        }, 50, 20, TimeUnit.MILLISECONDS);
        futureSailor = scheduler.scheduleWithFixedDelay(() -> {
            for (int i = 0; i < outstanding.size(); i++) {
                try {
                    HashKey result = outstanding.get(i).get(1, TimeUnit.MILLISECONDS);
                    outstanding.remove(i);
                    if (result != null) {
                        success.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                } catch (TimeoutException | InterruptedException e) {} catch (ExecutionException e) {
                    e.getCause().printStackTrace();
                }
            }
        }, 0, 50, TimeUnit.MILLISECONDS);
    }

    private void addTransaction(Duration txnWait, ScheduledExecutorService scheduler) {
        outstanding.add(node.submitTransaction(WellKnownDescriptions.BYTE_CONTENT.toHash(),
                                               ("transaction for: " + node.getNode().getId() + " : "
                                                       + counter.incrementAndGet()).getBytes(),
                                               txnWait, scheduler));
    }
}
