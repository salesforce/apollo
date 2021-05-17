/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.consortium.proto.BlockReplication;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Initial;
import com.salesfoce.apollo.consortium.proto.Initial.Builder;
import com.salesfoce.apollo.proto.ByteMessage;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.consortium.comms.ConsortiumClient;
import com.salesforce.apollo.consortium.support.SigningUtils;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;

/**
 * @author hal.hildebrand
 *
 */
public class MembershipTests {
    private static Map<HashKey, CertificateWithPrivateKey> certs;

    private static final Message GENESIS_DATA = ByteMessage.newBuilder()
                                                           .setContents(ByteString.copyFromUtf8("Give me food or give me slack or kill me"))
                                                           .build();

    private static final HashKey GENESIS_VIEW_ID = new HashKey(
            Conversion.hashOf("Give me food or give me slack or kill me".getBytes()));

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 11)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Utils.getMemberId(cert.getX509Certificate()), cert -> cert));
    }

    private final Map<Member, Consortium> consortium = new ConcurrentHashMap<>();
    private Context<Member>               context;
    private List<Member>                  members;
    private Store                         stateStore;
    private int                           testCardinality;

    @AfterEach
    public void after() {
        consortium.values().forEach(e -> e.stop());
        consortium.clear();
    }

    @BeforeEach
    public void before() {

        context = new Context<>(HashKey.ORIGIN, 3);

        stateStore = new Store(new MVStore.Builder().open());

        members = certs.values()
                       .stream()
                       .map(c -> new Member(c.getX509Certificate()))
                       .peek(m -> context.activate(m))
                       .collect(Collectors.toList());

    }

    @Test
    public void testBootstrapNoPrior() throws Exception {

    }

    @Test
    public void testCheckpointBootstrap() throws Exception {
    }

    public void testGenesisBootstrap() throws Exception {
        testCardinality = 3;
        TestChain testChain = new TestChain(stateStore);
        testChain.genesis()
                 .userBlocks(10)
                 .viewChange()
                 .userBlocks(10)
                 .viewChange()
                 .userBlocks(10)
                 .viewChange()
                 .userBlocks(10)
                 .viewChange()
                 .userBlocks(10)
                 .synchronizeView()
                 .viewChange()
                 .userBlocks(20)
                 .anchor()
                 .userBlocks(5);
        AtomicReference<CountDownLatch> processed = new AtomicReference<>(new CountDownLatch(testCardinality));
        initialize(testChain, processed);

        System.out.println("starting consortium");
        consortium.values().forEach(e -> e.start());

        assertTrue(processed.get().await(30, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    void initialize(TestChain testChain, AtomicReference<CountDownLatch> processed) {
        CommonCommunications<ConsortiumClient, Object> comms = buildComms(testChain);

        Router router = mock(Router.class);
        when(router.create(any(), any(), any(), any(), any(CreateClientCommunications.class))).thenReturn(comms);
        gatherConsortium(Duration.ofMillis(150), router, processed.get());
    }

    private CommonCommunications<ConsortiumClient, Object> buildComms(TestChain testChain) {

        ConsortiumClient client = mock(ConsortiumClient.class);

        when(client.sync(any())).then(new Answer<>() {
            @Override
            public ListenableFuture<Initial> answer(InvocationOnMock invocation) throws Throwable {
                SettableFuture<Initial> futureSailor = SettableFuture.create();
                Builder initial = Initial.newBuilder()
                                         .setCheckpoint(testChain.getSynchronizeCheckpoint().block)
                                         .setCheckpointView(testChain.getSynchronizeView().block)
                                         .setGenesis(testChain.getGenesis().block);
                futureSailor.set(initial.build());
                return futureSailor;
            }
        });
        when(client.fetchViewChain(any())).then(new Answer<>() {
            @Override
            public ListenableFuture<Blocks> answer(InvocationOnMock invocation) throws Throwable {
                SettableFuture<Blocks> futureSailor = SettableFuture.create();
                BlockReplication rep = invocation.getArgumentAt(0, BlockReplication.class);
                BloomFilter<Long> bff = BloomFilter.from(rep.getBlocksBff());
                Blocks.Builder blocks = Blocks.newBuilder();
                stateStore.fetchViewChain(bff, blocks, 1, rep.getFrom(), rep.getTo());
                futureSailor.set(blocks.build());
                return futureSailor;
            }
        });
        when(client.fetchBlocks(any())).then(new Answer<>() {
            @Override
            public ListenableFuture<Blocks> answer(InvocationOnMock invocation) throws Throwable {
                SettableFuture<Blocks> futureSailor = SettableFuture.create();
                BlockReplication rep = invocation.getArgumentAt(0, BlockReplication.class);
                BloomFilter<Long> bff = BloomFilter.from(rep.getBlocksBff());
                Blocks.Builder blocks = Blocks.newBuilder();
                stateStore.fetchBlocks(bff, blocks, 5, rep.getFrom(), rep.getTo());
                futureSailor.set(blocks.build());
                return futureSailor;
            }
        });

        @SuppressWarnings("unchecked")
        CommonCommunications<ConsortiumClient, Object> comms = mock(CommonCommunications.class);
        when(comms.apply(any(), any())).thenReturn(client);
        return comms;
    }

    private void gatherConsortium(Duration gossipDuration, Router comms, CountDownLatch processed) {
        Messenger.Parameters msgParameters = Messenger.Parameters.newBuilder()
                                                                 .setFalsePositiveRate(0.001)
                                                                 .setBufferSize(1000)
                                                                 .build();
        Executor cPipeline = Executors.newSingleThreadExecutor();
        Set<HashKey> decided = Collections.newSetFromMap(new ConcurrentHashMap<>());
        BiFunction<CertifiedBlock, Future<?>, HashKey> consensus = (c, f) -> {
            HashKey hash = new HashKey(Conversion.hashOf(c.getBlock().toByteString()));
            if (decided.add(hash)) {
                cPipeline.execute(() -> consortium.values().parallelStream().forEach(m -> {
                    m.process(c);
                    processed.countDown();
                }));
            }
            return hash;
        };
        TransactionExecutor executor = (h, et, c) -> {
            if (c != null) {
                c.accept(new HashKey(et.getHash()), null);
            }
        };
        members.stream()
               .map(m -> new Consortium(
                       Parameters.newBuilder()
                                 .setConsensus(consensus)
                                 .setMember(m)
                                 .setSignature(() -> SigningUtils.forSigning(certs.get(m.getId()).getPrivateKey(),
                                                                             Utils.secureEntropy()))
                                 .setContext(context)
                                 .setMsgParameters(msgParameters)
                                 .setMaxBatchByteSize(1024 * 1024)
                                 .setMaxBatchSize(1000)
                                 .setCommunications(comms)
                                 .setMaxBatchDelay(Duration.ofMillis(1000))
                                 .setGossipDuration(gossipDuration)
                                 .setViewTimeout(Duration.ofMillis(1500))
                                 .setSynchronizeTimeout(Duration.ofMillis(1500))
                                 .setJoinTimeout(Duration.ofSeconds(5))
                                 .setTransactonTimeout(Duration.ofSeconds(30))
                                 .setScheduler(Executors.newSingleThreadScheduledExecutor())
                                 .setExecutor(executor)
                                 .setGenesisData(GENESIS_DATA)
                                 .setGenesisViewId(GENESIS_VIEW_ID)
                                 .setDeltaCheckpointBlocks(5)
                                 .setCheckpointer(l -> {
                                     File temp;
                                     try {
                                         temp = File.createTempFile("foo", "bar");
                                         temp.deleteOnExit();
                                         try (FileOutputStream fos = new FileOutputStream(temp)) {
                                             fos.write("Give me food or give me slack or kill me".getBytes());
                                             fos.flush();
                                         }
                                     } catch (IOException e) {
                                         throw new IllegalStateException("Cannot create temp file", e);
                                     }

                                     return temp;
                                 })
                                 .build()))
               .peek(c -> context.activate(c.getMember()))
               .forEach(e -> consortium.put(e.getMember(), e));
    }
}
