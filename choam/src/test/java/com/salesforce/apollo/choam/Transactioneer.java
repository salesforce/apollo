/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.test.proto.ByteMessage;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.utils.Utils;

class Transactioneer {
    private final static Random entropy = new Random();
    private final static Logger log     = LoggerFactory.getLogger(Transactioneer.class);

    private final AtomicInteger              completed = new AtomicInteger();
    private final CountDownLatch             countdown;
    private final List<CompletableFuture<?>> inFlight  = new CopyOnWriteArrayList<>();
    private final int                        max;
    private final ScheduledExecutorService   scheduler;
    private final Session                    session;
    private final Duration                   timeout;
    private final ByteMessage                tx        = ByteMessage.newBuilder()
                                                                    .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                                                    .build();
    private final Executor                   txnCompletion;
    private final Executor                   txnExecutor;

    Transactioneer(Session session, Executor txnCompletion, Duration timeout, int max,
                   ScheduledExecutorService scheduler, CountDownLatch countdown, Executor txnScheduler) {
        this.session = session;
        this.timeout = timeout;
        this.max = max;
        this.scheduler = scheduler;
        this.countdown = countdown;
        this.txnExecutor = txnScheduler;
        this.txnCompletion = txnCompletion;
    }

    public int getCompleted() {
        return completed.get();
    }

    void decorate(CompletableFuture<?> fs) {
        final var futureSailor = new AtomicReference<CompletableFuture<?>>();
        futureSailor.set(fs.whenCompleteAsync((o, t) -> {
            inFlight.remove(futureSailor.get());
            if (t != null) {
                if (completed.get() < max) {
                    scheduler.schedule(() -> {
                        txnExecutor.execute(Utils.wrapped(() -> {
                            try {
                                decorate(session.submit(tx, timeout, scheduler));
                            } catch (InvalidTransaction e) {
                                throw new IllegalStateException(e);
                            }
                        }, log));
                    }, entropy.nextInt(100), TimeUnit.MILLISECONDS);
                }
            } else {
                if (completed.incrementAndGet() >= max) {
                    if (inFlight.size() == 0) {
                        countdown.countDown();
                    }
                } else {
                    txnExecutor.execute(Utils.wrapped(() -> {
                        try {
                            decorate(session.submit(tx, timeout, scheduler));
                        } catch (InvalidTransaction e) {
                            throw new IllegalStateException(e);
                        }
                    }, log));
                }
            }
        }, txnCompletion));
        inFlight.add(futureSailor.get());
    }

    void start() {
        scheduler.schedule(() -> {
            txnExecutor.execute(Utils.wrapped(() -> {
                try {
                    decorate(session.submit(tx, timeout, scheduler));
                } catch (InvalidTransaction e) {
                    throw new IllegalStateException(e);
                }
            }, log));
        }, 2, TimeUnit.SECONDS);
    }
}
