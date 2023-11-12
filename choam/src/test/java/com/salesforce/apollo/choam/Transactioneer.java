/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.test.proto.ByteMessage;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class Transactioneer {
    private final static Random                     entropy   = new Random();
    private final static Logger                     log       = LoggerFactory.getLogger(Transactioneer.class);
    private final static Executor                   executor  = Executors.newVirtualThreadPerTaskExecutor();
    private final static ScheduledExecutorService   scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual()
                                                                                                          .factory());
    private final        AtomicInteger              completed = new AtomicInteger();
    private final        CountDownLatch             countdown;
    private final        List<CompletableFuture<?>> inFlight  = new CopyOnWriteArrayList<>();
    private final        int                        max;
    private final        Session                    session;
    private final        Duration                   timeout;
    private final        ByteMessage                tx        = ByteMessage.newBuilder()
                                                                           .setContents(ByteString.copyFromUtf8(
                                                                           "Give me food or give me slack or kill me"))
                                                                           .build();
    private final        AtomicBoolean              finished  = new AtomicBoolean();

    Transactioneer(Session session, Duration timeout, int max, CountDownLatch countdown) {
        this.session = session;
        this.timeout = timeout;
        this.max = max;
        this.countdown = countdown;
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
                    scheduler.schedule(() -> executor.execute(Utils.wrapped(() -> {
                        try {
                            decorate(session.submit(tx, timeout));
                        } catch (InvalidTransaction e) {
                            throw new IllegalStateException(e);
                        }
                    }, log)), entropy.nextInt(100), TimeUnit.MILLISECONDS);
                }
            } else {
                if (completed.incrementAndGet() >= max) {
                    if (finished.compareAndSet(false, true)) {
                        countdown.countDown();
                    }
                } else {
                    executor.execute(Utils.wrapped(() -> {
                        try {
                            decorate(session.submit(tx, timeout));
                        } catch (InvalidTransaction e) {
                            throw new IllegalStateException(e);
                        }
                    }, log));
                }
            }
        }, executor));
        inFlight.add(futureSailor.get());
    }

    void start() {
        scheduler.schedule(() -> executor.execute(Utils.wrapped(() -> {
            try {
                decorate(session.submit(tx, timeout));
            } catch (InvalidTransaction e) {
                throw new IllegalStateException(e);
            }
        }, log)), 2, TimeUnit.SECONDS);
    }
}
