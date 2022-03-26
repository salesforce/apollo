/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesforce.apollo.choam.support.InvalidTransaction;

class Transactioneer {
    private final static Random entropy = new Random();

    private final AtomicInteger            completed = new AtomicInteger();
    private final CountDownLatch           countdown;
    private final AtomicInteger            inFlight  = new AtomicInteger();
    private final int                      max;
    private final ScheduledExecutorService scheduler;
    private final Session                  session;
    private final Duration                 timeout;
    private final ByteMessage              tx        = ByteMessage.newBuilder()
                                                                  .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                                                  .build();
    private final Executor                 txnScheduler;

    Transactioneer(Session session, Duration timeout, int max, ScheduledExecutorService scheduler,
                   CountDownLatch countdown, Executor txnScheduler) {
        this.session = session;
        this.timeout = timeout;
        this.max = max;
        this.scheduler = scheduler;
        this.countdown = countdown;
        this.txnScheduler = txnScheduler;
    }

    void decorate(CompletableFuture<?> fs) {
        inFlight.incrementAndGet();

        fs.whenCompleteAsync((o, t) -> {
            inFlight.decrementAndGet();

            if (t != null) {
                if (completed.get() < max) {
                    scheduler.schedule(() -> {
                        try {
                            decorate(session.submit(txnScheduler, tx, timeout, scheduler));
                        } catch (InvalidTransaction e) {
                            e.printStackTrace();
                        }
                    }, entropy.nextInt(100), TimeUnit.MILLISECONDS);
                }
            } else {
                if (completed.incrementAndGet() >= max) {
                    if (inFlight.get() == 0) {
                        countdown.countDown();
                    }
                } else {
                    try {
                        decorate(session.submit(txnScheduler, tx, timeout, scheduler));
                    } catch (InvalidTransaction e) {
                        e.printStackTrace();
                    }
                }
            }
        }, txnScheduler);
    }

    void start() {
        scheduler.schedule(() -> {
            try {
                decorate(session.submit(txnScheduler, tx, timeout, scheduler));
            } catch (InvalidTransaction e) {
                throw new IllegalStateException(e);
            }
        }, 2, TimeUnit.SECONDS);
    }
}
