/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.choam.support.InvalidTransaction;

class Transactioneer {
    private final static Random              entropy   = new Random();
    private final AtomicInteger              completed = new AtomicInteger();
    private final CountDownLatch             countdown;
    private final Executor                   executor;
    private final List<CompletableFuture<?>> inFlight  = new CopyOnWriteArrayList<>();
    private final int                        max;
    private final Mutator                    mutator;
    private final ScheduledExecutorService   scheduler;
    private final Duration                   timeout;
    private final Supplier<Txn>              update;

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
        return inFlight.size();
    }

    void decorate(CompletableFuture<?> fs) {
        final var futureSailor = new AtomicReference<CompletableFuture<?>>();
        futureSailor.set(fs.whenCompleteAsync((o, t) -> {
            inFlight.remove(futureSailor.get());
            if (t != null) {
                if (completed.get() < max) {
                    scheduler.schedule(() -> {
                        try {
                            decorate(mutator.getSession().submit(update.get(), timeout, scheduler));
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
                            decorate(mutator.getSession().submit(update.get(), timeout, scheduler));
                        } catch (InvalidTransaction e) {
                            e.printStackTrace();
                        }
                    }, entropy.nextInt(100), TimeUnit.MILLISECONDS);
                } else if (complete >= max) {
                    if (inFlight.size() == 0) {
                        countdown.countDown();
                    }
                }
            }
        }, executor));
        inFlight.add(futureSailor.get());
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
