/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.ByteMessage;
import com.salesforce.apollo.avalanche.Processor.TimedProcessor;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 222
 */
public class Transactioneer {
    private final AtomicInteger                   counter     = new AtomicInteger();
    private final AtomicInteger                   failed      = new AtomicInteger();
    private volatile ScheduledFuture<?>           futureSailor;
    private final TimedProcessor                  processor;
    private final Set<CompletableFuture<HashKey>> outstanding = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final AtomicInteger                   success     = new AtomicInteger();
    private final AtomicInteger                   limit       = new AtomicInteger();
    private final CountDownLatch                  gate;
    private final AtomicBoolean                   complete    = new AtomicBoolean();

    public Transactioneer(TimedProcessor p, int limit, CountDownLatch gate) {
        this.processor = p;
        this.limit.set(limit);
        this.gate = gate;
    }

    public int getFailed() {
        return failed.get();
    }

    public HashKey getId() {
        return processor.getAvalanche().getNode().getId();
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
                limit.addAndGet(-2);
            }
            if (limit.get() <= 0) {
                if (complete.compareAndExchange(false, true)) {
                    if (gate != null) {
                        gate.countDown();
                    }
                }
            }
        }, 50, 5, TimeUnit.MILLISECONDS);
    }

    private void addTransaction(Duration txnWait, ScheduledExecutorService scheduler) {
        CompletableFuture<HashKey> future = processor.submitTransaction((ByteMessage.newBuilder()
                                                                                    .setContents(ByteString.copyFromUtf8("transaction for: "
                                                                                            + processor.getAvalanche()
                                                                                                       .getNode()
                                                                                                       .getId()
                                                                                            + " : "
                                                                                            + counter.incrementAndGet()))
                                                                                    .build()),
                                                                        txnWait, scheduler);
        future.whenComplete((hash, error) -> {
            outstanding.remove(future);
            if (hash != null) {
                success.incrementAndGet();
            } else {
                failed.incrementAndGet();
            }
        });
        outstanding.add(future);
    }
}
