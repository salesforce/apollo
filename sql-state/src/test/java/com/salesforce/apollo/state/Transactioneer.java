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
import java.util.function.Supplier;

import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.support.InvalidTransaction;

class Transactioneer {
    private final static Random            entropy   = new Random();
    private final Supplier<Txn>            update;
    private final AtomicInteger            completed = new AtomicInteger();
    private final CountDownLatch           countdown;
    private final AtomicInteger            inFlight  = new AtomicInteger();
    private final int                      max;
    private final Mutator                  mutator;
    private final ScheduledExecutorService scheduler;
    private final Duration                 timeout;

    public Transactioneer(Supplier<Txn> update, Mutator mutator, Duration timeout, int max, CountDownLatch countdown,
                          ScheduledExecutorService txScheduler) {
        this.update = update;
        this.timeout = timeout;
        this.max = max;
        this.countdown = countdown;
        this.scheduler = txScheduler;
        this.mutator = mutator;
    }

    public int completed() {
        return completed.get();
    }

    void decorate(CompletableFuture<?> fs) {
        inFlight.incrementAndGet();
        fs.whenComplete((o, t) -> {
            inFlight.decrementAndGet();
            if (t != null) {
                if (t instanceof CompletionException e) {
                    if (!(e.getCause() instanceof TimeoutException)) {
                        System.out.println(e.getCause().toString());
                    }
                }

                if (completed.get() < max) {
                    scheduler.schedule(() -> {
                        try {
                            decorate(mutator.getSession()
                                            .submit(ForkJoinPool.commonPool(), update.get(), timeout, scheduler));
                        } catch (InvalidTransaction e) {
                            e.printStackTrace();
                        }
                    }, entropy.nextInt(100), TimeUnit.MILLISECONDS);
                }
            } else {
                final var complete = completed.incrementAndGet();
                if (complete < max) {
                    scheduler.schedule(() -> {
                        try {
                            decorate(mutator.getSession()
                                            .submit(ForkJoinPool.commonPool(), update.get(), timeout, scheduler));
                        } catch (InvalidTransaction e) {
                            e.printStackTrace();
                        }
                    }, entropy.nextInt(100), TimeUnit.MILLISECONDS);
                } else if (complete >= max) {
                    if (inFlight.get() == 0) {
                        countdown.countDown();
                    }
                }
            }
        });
    }

    void start() {
        scheduler.schedule(() -> {
            try {
                decorate(mutator.getSession().submit(ForkJoinPool.commonPool(), update.get(), timeout, scheduler));
            } catch (InvalidTransaction e) {
                throw new IllegalStateException(e);
            }
        }, 2, TimeUnit.SECONDS);
    }
}
