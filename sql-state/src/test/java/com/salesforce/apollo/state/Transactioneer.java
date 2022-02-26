/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.salesforce.apollo.choam.support.InvalidTransaction;

class Transactioneer {
    /**
     * 
     */
    private final AbstractLifecycleTest abstractLifecycleTest;

    private final static Random entropy = new Random();

    private final AtomicInteger            completed = new AtomicInteger();
    private final CountDownLatch           countdown;
    private final AtomicInteger            failed    = new AtomicInteger();
    private final AtomicInteger            lineTotal;
    private final int                      max;
    private final Mutator                  mutator;
    private final ScheduledExecutorService scheduler;
    private final Duration                 timeout;

    public Transactioneer(AbstractLifecycleTest abstractLifecycleTest, Mutator mutator, Duration timeout, AtomicInteger lineTotal, int max,
                          CountDownLatch countdown, ScheduledExecutorService txScheduler) {
        this.abstractLifecycleTest = abstractLifecycleTest;
        this.timeout = timeout;
        this.lineTotal = lineTotal;
        this.max = max;
        this.countdown = countdown;
        this.scheduler = txScheduler;
        this.mutator = mutator;
    }

    public int completed() {
        return completed.get();
    }

    void decorate(CompletableFuture<?> fs) {
        fs.whenCompleteAsync((o, t) -> {
            if (t != null) {
                failed.incrementAndGet();
                if (t instanceof CompletionException e) {
                    if (!(e.getCause() instanceof TimeoutException)) {
                        e.getCause().printStackTrace();
                    }
                }

                if (completed.get() < max) {
                    scheduler.schedule(() -> {
                        try {
                            decorate(mutator.getSession()
                                            .submit(ForkJoinPool.commonPool(), this.abstractLifecycleTest.update(entropy, mutator), timeout,
                                                    scheduler));
                        } catch (InvalidTransaction e) {
                            e.printStackTrace();
                        }
                    }, entropy.nextInt(100), TimeUnit.MILLISECONDS);
                }
            } else {
                final int tot = lineTotal.incrementAndGet();
                if (tot % 100 == 0) {
                    System.out.println(".");
                } else {
                    System.out.print(".");
                }
                final var complete = completed.incrementAndGet();
                if (complete < max) {
                    scheduler.schedule(() -> {
                        try {
                            decorate(mutator.getSession()
                                            .submit(ForkJoinPool.commonPool(), this.abstractLifecycleTest.update(entropy, mutator), timeout,
                                                    scheduler));
                        } catch (InvalidTransaction e) {
                            e.printStackTrace();
                        }
                    }, entropy.nextInt(100), TimeUnit.MILLISECONDS);
                } else if (complete >= max) {
                    countdown.countDown();
                }
            }
        });
    }

    void start() {
        scheduler.schedule(() -> {
            try {
                decorate(mutator.getSession()
                                .submit(ForkJoinPool.commonPool(), this.abstractLifecycleTest.update(entropy, mutator), timeout, scheduler));
            } catch (InvalidTransaction e) {
                throw new IllegalStateException(e);
            }
        }, 2, TimeUnit.SECONDS);
    }
}