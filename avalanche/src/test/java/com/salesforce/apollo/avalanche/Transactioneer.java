/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private final AtomicInteger                    counter     = new AtomicInteger();
    private final AtomicInteger                    failed      = new AtomicInteger();
    private volatile ScheduledFuture<?>            futureSailor;
    private final TimedProcessor                   processor;
    private final List<CompletableFuture<HashKey>> outstanding = new CopyOnWriteArrayList<>();
    private final AtomicInteger                    success     = new AtomicInteger();
    private final AtomicInteger                    limit       = new AtomicInteger();

    public Transactioneer(TimedProcessor p, int limit) {
        this.processor = p;
        this.limit.set(limit);
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
                if (limit.decrementAndGet() > 0) {
                    addTransaction(txnWait, scheduler);
                }
                if (limit.decrementAndGet() > 0) {
                    addTransaction(txnWait, scheduler);
                }
            }
        }, 50, 5, TimeUnit.MILLISECONDS);
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
                } catch (TimeoutException | InterruptedException e) {
                } catch (ExecutionException e) {
                    e.getCause().printStackTrace();
                }
            }
        }, 0, 50, TimeUnit.MILLISECONDS);
    }

    private void addTransaction(Duration txnWait, ScheduledExecutorService scheduler) {
        outstanding.add(processor.submitTransaction((ByteMessage.newBuilder()
                                                                .setContents(ByteString.copyFromUtf8("transaction for: "
                                                                        + processor.getAvalanche().getNode().getId()
                                                                        + " : " + counter.incrementAndGet()))
                                                                .build()),
                                                    txnWait, scheduler));
    }
}
