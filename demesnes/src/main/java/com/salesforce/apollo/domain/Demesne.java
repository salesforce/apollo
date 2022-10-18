/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.domain;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.ObjectHandle;
import org.graalvm.nativeimage.ObjectHandles;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

import com.salesfoce.apollo.choam.proto.Foundation;
import com.salesfoce.apollo.choam.proto.FoundationSeal;
import com.salesforce.apollo.archipelago.LocalServer;
import com.salesforce.apollo.archipelago.Router;
import com.salesforce.apollo.archipelago.ServerConnectionCache;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.Builder;
import com.salesforce.apollo.choam.Parameters.ProducerParameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.fireflies.View.Seed;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.model.Domain.TransactionConfiguration;
import com.salesforce.apollo.model.ProcessDomain;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class Demesne {
    private static final int    CARDINALITY     = 5;
    private static final Digest GENESIS_VIEW_ID = DigestAlgorithm.DEFAULT.digest("Give me food or give me slack or kill me".getBytes());

    @CEntryPoint
    public static ObjectHandle launch(@CEntryPoint.IsolateThreadContext IsolateThread renderingContext,
                                      CCharPointer cString) {
        /* Convert the C string to the target Java string. */
        String targetString = CTypeConversion.toJavaString(cString);
        /*
         * Encapsulate the target string in a handle that can be returned back to the
         * source isolate.
         */
        try {
            final var demesne = new Demesne();
            demesne.before();
            demesne.smokin();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ObjectHandles.getGlobal().create(targetString);
    }

    private final List<ProcessDomain>        domains = new ArrayList<>();
    private final Map<ProcessDomain, Router> routers = new HashMap<>();

    public void before() throws Exception {
        var scheduler = Executors.newScheduledThreadPool(2);
        var ffParams = com.salesforce.apollo.fireflies.Parameters.newBuilder();
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        final var prefix = UUID.randomUUID().toString();
        Path checkpointDirBase = Path.of("target", "ct-chkpoints-" + Entropy.nextBitsStreamLong());
        Utils.clean(checkpointDirBase.toFile());
        var params = params();
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(params.getDigestAlgorithm()), entropy);

        var identities = IntStream.range(0, CARDINALITY).mapToObj(i -> {
            try {
                return stereotomy.newIdentifier().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }).collect(Collectors.toMap(controlled -> controlled.getIdentifier().getDigest(), controlled -> controlled));

        Digest group = DigestAlgorithm.DEFAULT.getOrigin();
        var foundation = Foundation.newBuilder();
        identities.keySet().forEach(d -> foundation.addMembership(d.toDigeste()));
        var sealed = FoundationSeal.newBuilder().setFoundation(foundation).build();
        TransactionConfiguration txnConfig = new TransactionConfiguration(Executors.newFixedThreadPool(2),
                                                                          Executors.newSingleThreadScheduledExecutor());
        identities.forEach((digest, id) -> {
            var context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getLast(), CARDINALITY, 0.2, 3);
            final var member = new ControlledIdentifierMember(id);
            var localRouter = new LocalServer(prefix, member,
                                              Executors.newSingleThreadExecutor()).router(ServerConnectionCache.newBuilder().setTarget(30), ForkJoinPool.commonPool());
            var node = new ProcessDomain(group, member, params, "jdbc:h2:mem:", checkpointDirBase,
                                         RuntimeParameters.newBuilder()
                                                          .setFoundation(sealed)
                                                          .setScheduler(scheduler)
                                                          .setContext(context)
                                                          .setExec(ForkJoinPool.commonPool())
                                                          .setCommunications(localRouter),
                                         new InetSocketAddress(0), ffParams, txnConfig);
            domains.add(node);
            routers.put(node, localRouter);
            localRouter.start();
        });
    }

    public void smokin() throws Exception {
        final var gossipDuration = Duration.ofMillis(10);
        final var countdown = new CountDownLatch(domains.size());
        final var seeds = Collections.singletonList(new Seed(domains.get(0).getMember().getEvent().getCoordinates(),
                                                             new InetSocketAddress(0)));
        domains.forEach(d -> {
            d.getFoundation().register((context, viewId, joins, leaves) -> {
                if (context.totalCount() == CARDINALITY) {
                    System.out.println(String.format("Full view: %s members: %s on: %s", viewId, context.totalCount(),
                                                     d.getMember().getId()));
                    countdown.countDown();
                } else {
                    System.out.println(String.format("Members joining: %s members: %s on: %s", viewId,
                                                     context.totalCount(), d.getMember().getId()));
                }
            });
        });
        // start seed
        final var scheduler = Executors.newScheduledThreadPool(2);
        final var started = new AtomicReference<>(new CountDownLatch(1));

        domains.get(0)
               .getFoundation()
               .start(() -> started.get().countDown(), gossipDuration, Collections.emptyList(), scheduler);

        started.set(new CountDownLatch(CARDINALITY - 1));
        domains.subList(1, domains.size()).forEach(d -> {
            d.getFoundation().start(() -> started.get().countDown(), gossipDuration, seeds, scheduler);
        });
    }

    private Builder params() {
        var params = Parameters.newBuilder()
                               .setGenesisViewId(GENESIS_VIEW_ID)
                               .setGossipDuration(Duration.ofMillis(50))
                               .setProducer(ProducerParameters.newBuilder()
                                                              .setGossipDuration(Duration.ofMillis(50))
                                                              .setBatchInterval(Duration.ofMillis(100))
                                                              .setMaxBatchByteSize(1024 * 1024)
                                                              .setMaxBatchCount(3000)
                                                              .build())
                               .setCheckpointBlockDelta(200);

        params.getProducer().ethereal().setNumberOfEpochs(5);
        return params;
    }

}
