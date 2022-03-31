/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.salesfoce.apollo.ethereal.proto.ByteMessage;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.Utils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * @author hal.hildebrand
 *
 */
public class SessionTest {
    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(SessionTest.class).error("Error on thread: {}", t.getName(), e);
        });
    }

    @Test
    public void func() throws Exception {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        Context<Member> context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), 9, 0.2, 2);
        Parameters params = Parameters.newBuilder()
                                      .build(RuntimeParameters.newBuilder()
                                                              .setContext(context)
                                                              .setMember(new SigningMemberImpl(Utils.getMember(0)))
                                                              .build());
        @SuppressWarnings("unchecked")
        Function<SubmittedTransaction, ListenableFuture<Status>> service = stx -> {
            ForkJoinPool.commonPool().execute(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                try {
                    stx.onCompletion()
                       .complete(ByteMessage.parseFrom(stx.transaction().getContent()).getContents().toStringUtf8());
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException(e);
                }
            });
            SettableFuture<Status> f = SettableFuture.create();
            f.set(Status.OK);
            return f;
        };
        Session session = new Session(params, service);
        final String content = "Give me food or give me slack or kill me";
        Message tx = ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8(content)).build();
        var result = session.submit(tx, null, exec);
        assertEquals(1, session.submitted());
        assertEquals(content, result.get(1, TimeUnit.SECONDS));
        assertEquals(0, session.submitted());
    }

    @Test
    public void scalingTest() throws Exception {
        var exec = Executors.newFixedThreadPool(2);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Context<Member> context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), 9, 0.2, 3);
        Parameters params = Parameters.newBuilder()
                                      .build(RuntimeParameters.newBuilder()
                                                              .setContext(context)
                                                              .setMember(new SigningMemberImpl(Utils.getMember(0)))
                                                              .build());

        @SuppressWarnings("unchecked")
        Function<SubmittedTransaction, ListenableFuture<Status>> service = stx -> {
            exec.execute(() -> {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                }
                try {
                    stx.onCompletion()
                       .complete(ByteMessage.parseFrom(stx.transaction().getContent()).getContents().toStringUtf8());
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException(e);
                }
            });
            SettableFuture<Status> f = SettableFuture.create();
            f.set(Status.OK);
            return f;
        };

        MetricRegistry reg = new MetricRegistry();
        Timer latency = reg.timer("Transaction latency");

        Session session = new Session(params, service);
        List<CompletableFuture<?>> futures = new ArrayList<>();
        IntStream.range(0, 10000).forEach(i -> {
            final var time = latency.time();
            final String content = "Give me food or give me slack or kill me";
            Message tx = ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8(content)).build();
            CompletableFuture<Object> result;
            try {
                result = session.submit(tx, null, scheduler).whenComplete((r, t) -> {
                    if (t != null) {
                        if (t instanceof CompletionException ce) {
                            reg.counter(t.getCause().getClass().getSimpleName()).inc();
                        } else {
                            reg.counter(t.getClass().getSimpleName()).inc();
                        }
                    } else {
                        time.close();
                    }
                });
                futures.add(result);
            } catch (InvalidTransaction e) {
                e.printStackTrace();
            }
        });
        for (CompletableFuture<?> f : futures) {
            try {
                f.get(10, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof StatusRuntimeException sre) {

                }
            }
        }
        System.out.println();
        ConsoleReporter.forRegistry(reg)
                       .convertRatesTo(TimeUnit.SECONDS)
                       .convertDurationsTo(TimeUnit.MILLISECONDS)
                       .build()
                       .report();
    }
}
