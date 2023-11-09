/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class Transactioneer {
    private final static Random                             entropy   = new Random();
    private final static Logger                             log       = LoggerFactory.getLogger(Transactioneer.class);
    private final        AtomicInteger                      completed = new AtomicInteger();
    private final        CountDownLatch                     countdown;
    private final        Executor                           executor;
    private final        AtomicReference<CompletableFuture> inFlight  = new AtomicReference<>();
    private final        int                                max;
    private final        Mutator                            mutator;
    private final        ScheduledExecutorService           scheduler;
    private final        Duration                           timeout;
    private final        Supplier<Txn>                      update;
    private final        AtomicBoolean                      finished  = new AtomicBoolean();

    public Transactioneer(Supplier<Txn> update, Mutator mutator, Duration timeout, int max, Executor executor,
                          CountDownLatch countdown, ScheduledExecutorService txScheduler) {
        this.update = update;
        this.timeout = timeout;
        this.max = max;
        this.countdown = countdown;
        this.scheduler = txScheduler;
        this.mutator = mutator;
        this.executor = executor;
    }

    public int completed() {
        return completed.get();
    }

    public int inFlight() {
        return inFlight.get() != null ? 1 : 0;
    }

    void decorate(CompletableFuture<?> fs) {
        final var futureSailor = new AtomicReference<CompletableFuture<?>>();
        futureSailor.set(fs.whenComplete((o, t) -> {
            inFlight.set(null);
            if (t != null) {
                if (completed.get() < max) {
                    scheduler.schedule(() -> {
                        executor.execute(Utils.wrapped(() -> {
                            try {
                                decorate(mutator.getSession().submit(update.get(), timeout, scheduler));
                            } catch (InvalidTransaction e) {
                                e.printStackTrace();
                            }
                        }, log));
                    }, entropy.nextInt(100), TimeUnit.MILLISECONDS);
                }
            } else {
                final var complete = completed.incrementAndGet();
                final var finish = finished;

                if (complete < max) {
                    scheduler.schedule(() -> {
                        executor.execute(Utils.wrapped(() -> {
                            try {
                                decorate(mutator.getSession().submit(update.get(), timeout, scheduler));
                            } catch (InvalidTransaction e) {
                                e.printStackTrace();
                            }
                        }, log));
                    }, entropy.nextInt(100), TimeUnit.MILLISECONDS);
                } else if (finish.compareAndSet(false, true)) {
                    countdown.countDown();
                }
            }
        }));
        inFlight.set(futureSailor.get());
    }

    void start() {
        scheduler.schedule(() -> {
            try {
                decorate(mutator.getSession().submit(update.get(), timeout, scheduler));
            } catch (InvalidTransaction e) {
                throw new IllegalStateException(e);
            }
        }, 2, TimeUnit.SECONDS);
    }
}
