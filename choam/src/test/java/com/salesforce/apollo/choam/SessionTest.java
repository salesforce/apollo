/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.proto.Block;
import com.salesforce.apollo.choam.proto.CertifiedBlock;
import com.salesforce.apollo.choam.proto.Header;
import com.salesforce.apollo.choam.proto.SubmitResult;
import com.salesforce.apollo.choam.proto.SubmitResult.Result;
import com.salesforce.apollo.choam.support.HashedCertifiedBlock;
import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.choam.support.SubmittedTransaction;
import com.salesforce.apollo.context.StaticContext;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.test.proto.ByteMessage;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author hal.hildebrand
 */
public class SessionTest {
    static {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
            LoggerFactory.getLogger(SessionTest.class).error("Error on thread: {}", t.getName(), e);
        });
    }

    @Test
    public void func() throws Exception {
        var context = new StaticContext<Member>(DigestAlgorithm.DEFAULT.getOrigin(), 0.1, Collections.emptyList(), 2);
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var params = Parameters.newBuilder()
                               .build(RuntimeParameters.newBuilder()
                                                       .setContext(context)
                                                       .setMember(new ControlledIdentifierMember(
                                                       new StereotomyImpl(new MemKeyStore(),
                                                                          new MemKERL(DigestAlgorithm.DEFAULT),
                                                                          entropy).newIdentifier()))
                                                       .build());
        var gate = new CountDownLatch(1);
        @SuppressWarnings("unchecked")
        Function<SubmittedTransaction, SubmitResult> service = stx -> {
            ForkJoinPool.commonPool().execute(() -> {
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                try {
                    stx.onCompletion()
                       .complete(ByteMessage.parseFrom(stx.transaction().getContent()).getContents().toStringUtf8());
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException(e);
                }
            });
            return SubmitResult.newBuilder().setResult(Result.PUBLISHED).build();
        };
        Session session = new Session(params, service,
                                      Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory()));
        session.setView(new HashedCertifiedBlock(DigestAlgorithm.DEFAULT, CertifiedBlock.newBuilder()
                                                                                        .setBlock(Block.newBuilder()
                                                                                                       .setHeader(
                                                                                                       Header.newBuilder()
                                                                                                             .setHeight(
                                                                                                             100)))
                                                                                        .build()));
        final String content = "Give me food or give me slack or kill me";
        Message tx = ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8(content)).build();
        var result = session.submit(tx, null);
        assertEquals(1, session.submitted());
        gate.countDown();
        assertEquals(content, result.get(1, TimeUnit.SECONDS));
        assertEquals(0, session.submitted());
    }

    @Test
    public void scalingTest() throws Exception {
        var exec = Executors.newVirtualThreadPerTaskExecutor();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, Thread.ofVirtual().factory());
        var context = new StaticContext<Member>(DigestAlgorithm.DEFAULT.getOrigin(), 0.1, Collections.emptyList(), 3);
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);
        Parameters params = Parameters.newBuilder()
                                      .build(RuntimeParameters.newBuilder()
                                                              .setContext(context)
                                                              .setMember(new ControlledIdentifierMember(
                                                              stereotomy.newIdentifier()))
                                                              .build());

        @SuppressWarnings("unchecked")
        Function<SubmittedTransaction, SubmitResult> service = stx -> {
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
            return SubmitResult.newBuilder().setResult(Result.PUBLISHED).build();
        };

        MetricRegistry reg = new MetricRegistry();
        Timer latency = reg.timer("Transaction latency");

        Session session = new Session(params, service, scheduler);
        session.setView(new HashedCertifiedBlock(DigestAlgorithm.DEFAULT, CertifiedBlock.newBuilder()
                                                                                        .setBlock(Block.newBuilder()
                                                                                                       .setHeader(
                                                                                                       Header.newBuilder()
                                                                                                             .setHeight(
                                                                                                             100)))
                                                                                        .build()));
        List<CompletableFuture<?>> futures = new ArrayList<>();
        IntStream.range(0, 10000).forEach(i -> {
            final var time = latency.time();
            final String content = "Give me food or give me slack or kill me";
            Message tx = ByteMessage.newBuilder().setContents(ByteString.copyFromUtf8(content)).build();
            CompletableFuture<Object> result;
            try {
                result = session.submit(tx, null).whenComplete((r, t) -> {
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
        if (Boolean.getBoolean("reportMetrics")) {
            ConsoleReporter.forRegistry(reg)
                           .convertRatesTo(TimeUnit.SECONDS)
                           .convertDurationsTo(TimeUnit.MILLISECONDS)
                           .build()
                           .report();
        }
    }
}
