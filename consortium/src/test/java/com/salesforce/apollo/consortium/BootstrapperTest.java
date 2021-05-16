/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.salesfoce.apollo.consortium.proto.BlockReplication;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.Initial;
import com.salesfoce.apollo.consortium.proto.Initial.Builder;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.comms.ConsortiumClient;
import com.salesforce.apollo.consortium.support.Bootstrapper;
import com.salesforce.apollo.consortium.support.HashedCertifiedBlock;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Pair;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;

/**
 * @author hal.hildebrand
 *
 */
public class BootstrapperTest {
    private static Map<HashKey, CertificateWithPrivateKey> certs;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 11)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Utils.getMemberId(cert.getX509Certificate()), cert -> cert));
    }

    @Test
    public void smoke() throws Exception {
        Context<Member> context = new Context<>(HashKey.ORIGIN, 3);

        Store bootstrapStore = new Store(new MVStore.Builder().open());

        List<Member> members = certs.values()
                                    .stream()
                                    .map(c -> new Member(c.getX509Certificate()))
                                    .peek(m -> context.activate(m))
                                    .collect(Collectors.toList());

        TestChain testChain = new TestChain(bootstrapStore);
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
                 .checkpoint()
                 .userBlocks(10)
                 .synchronizeView()
                 .userBlocks(10)
                 .synchronizeCheckpoint()
                 .userBlocks(5)
                 .viewChange()
                 .userBlocks(20)
                 .anchor()
                 .userBlocks(5);

        HashedCertifiedBlock lastBlock = testChain.getLastBlock();

        bootstrapStore.validate(lastBlock.height(), 0);
        bootstrapStore.validateViewChain(testChain.getSynchronizeView().height());

        Member member = members.get(0);
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
                bootstrapStore.fetchViewChain(bff, blocks, 1, rep.getFrom(), rep.getTo());
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
                bootstrapStore.fetchBlocks(bff, blocks, 5, rep.getFrom(), rep.getTo());
                futureSailor.set(blocks.build());
                return futureSailor;
            }
        });

        @SuppressWarnings("unchecked")
        CommonCommunications<ConsortiumClient, Service> comms = mock(CommonCommunications.class);
        when(comms.apply(any(), any())).thenReturn(client);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Duration duration = Duration.ofMillis(100);
        Store store = new Store(new MVStore.Builder().open());

        Bootstrapper boot = new Bootstrapper(testChain.getAnchor(), member, context, comms, 0.15, store, 5, scheduler,
                100, duration, 100);

        CompletableFuture<Pair<HashedCertifiedBlock, HashedCertifiedBlock>> syncFuture = boot.synchronize();
        syncFuture.get(10, TimeUnit.SECONDS);
    }

}
