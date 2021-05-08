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
import com.salesfoce.apollo.consortium.proto.Block;
import com.salesfoce.apollo.consortium.proto.BlockReplication;
import com.salesfoce.apollo.consortium.proto.Blocks;
import com.salesfoce.apollo.consortium.proto.Body;
import com.salesfoce.apollo.consortium.proto.BodyType;
import com.salesfoce.apollo.consortium.proto.CertifiedBlock;
import com.salesfoce.apollo.consortium.proto.Header;
import com.salesfoce.apollo.consortium.proto.Initial;
import com.salesfoce.apollo.consortium.proto.Initial.Builder;
import com.salesfoce.apollo.consortium.proto.Synchronize;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.Service;
import com.salesforce.apollo.consortium.comms.BootstrapClient;
import com.salesforce.apollo.consortium.support.Bootstrapper;
import com.salesforce.apollo.consortium.support.HashedCertifiedBlock;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
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

    @SuppressWarnings("unused")
    @Test
    public void smoke() throws Exception {
        Context<Member> context = new Context<>(HashKey.ORIGIN, 3);

        Store bootstrapStore = new Store(new MVStore.Builder().open());

        List<Member> members = certs.values()
                                    .stream()
                                    .map(c -> new Member(c.getX509Certificate()))
                                    .peek(m -> context.activate(m))
                                    .collect(Collectors.toList());

        HashedCertifiedBlock genesis = new HashedCertifiedBlock(
                CertifiedBlock.newBuilder()
                              .setBlock(Block.newBuilder()
                                             .setHeader(Header.newBuilder().setHeight(0))
                                             .setBody(Body.newBuilder().setType(BodyType.GENESIS))
                                             .build())
                              .build());
        bootstrapStore.put(genesis.hash, genesis.block);
        HashedCertifiedBlock lastBlock = genesis;
        HashedCertifiedBlock lastView = genesis;
        HashedCertifiedBlock checkpoint = genesis;

        for (int i = 0; i < 10; i++) {
            HashedCertifiedBlock block = new HashedCertifiedBlock(
                    CertifiedBlock.newBuilder()
                                  .setBlock(Block.newBuilder()
                                                 .setHeader(Header.newBuilder()
                                                                  .setHeight(lastBlock.height() + 1)
                                                                  .setLastCheckpoint(genesis.height())
                                                                  .setLastReconfig(lastView.height())
                                                                  .setPrevious(lastBlock.hash.toByteString()))
                                                 .setBody(Body.newBuilder().setType(BodyType.USER))
                                                 .build())
                                  .build());
            bootstrapStore.put(block.hash, block.block);
            lastBlock = block;
        }

        lastBlock = new HashedCertifiedBlock(
                CertifiedBlock.newBuilder()
                              .setBlock(Block.newBuilder()
                                             .setHeader(Header.newBuilder()
                                                              .setHeight(lastBlock.height() + 1)
                                                              .setLastCheckpoint(genesis.height())
                                                              .setLastReconfig(lastView.height())
                                                              .setPrevious(lastBlock.hash.toByteString()))
                                             .setBody(Body.newBuilder().setType(BodyType.RECONFIGURE))
                                             .build())
                              .build());
        bootstrapStore.put(lastBlock.hash, lastBlock.block);

        lastView = lastBlock;

        for (int i = 0; i < 20; i++) {
            HashedCertifiedBlock block = new HashedCertifiedBlock(
                    CertifiedBlock.newBuilder()
                                  .setBlock(Block.newBuilder()
                                                 .setHeader(Header.newBuilder()
                                                                  .setHeight(lastBlock.height() + 1)
                                                                  .setLastCheckpoint(genesis.height())
                                                                  .setLastReconfig(lastView.height())
                                                                  .setPrevious(lastBlock.hash.toByteString()))
                                                 .setBody(Body.newBuilder().setType(BodyType.USER))
                                                 .build())
                                  .build());
            bootstrapStore.put(block.hash, block.block);
            lastBlock = block;
        }

        lastBlock = new HashedCertifiedBlock(
                CertifiedBlock.newBuilder()
                              .setBlock(CollaboratorContext.generateBlock(checkpoint, lastBlock.height()
                                      + 1, lastBlock.hash.bytes(), CollaboratorContext.body(BodyType.CHECKPOINT, CollaboratorContext.checkpoint(lastBlock.height() + 1, null, 0)), lastView))
                              .build());
        bootstrapStore.put(lastBlock.hash, lastBlock.block);

        checkpoint = lastBlock;

        for (int i = 0; i < 5; i++) {
            HashedCertifiedBlock block = new HashedCertifiedBlock(
                    CertifiedBlock.newBuilder()
                                  .setBlock(Block.newBuilder()
                                                 .setHeader(Header.newBuilder()
                                                                  .setHeight(lastBlock.height() + 1)
                                                                  .setLastCheckpoint(checkpoint.height())
                                                                  .setLastReconfig(lastView.height())
                                                                  .setPrevious(lastBlock.hash.toByteString()))
                                                 .setBody(Body.newBuilder().setType(BodyType.USER))
                                                 .build())
                                  .build());
            bootstrapStore.put(block.hash, block.block);
            lastBlock = block;
        }

        lastBlock = new HashedCertifiedBlock(
                CertifiedBlock.newBuilder()
                              .setBlock(Block.newBuilder()
                                             .setHeader(Header.newBuilder()
                                                              .setHeight(lastBlock.height() + 1)
                                                              .setLastCheckpoint(checkpoint.height())
                                                              .setLastReconfig(lastView.height())
                                                              .setPrevious(lastBlock.hash.toByteString()))
                                             .setBody(Body.newBuilder().setType(BodyType.RECONFIGURE))
                                             .build())
                              .build());
        bootstrapStore.put(lastBlock.hash, lastBlock.block);

        for (int i = 0; i < 20; i++) {
            HashedCertifiedBlock block = new HashedCertifiedBlock(
                    CertifiedBlock.newBuilder()
                                  .setBlock(Block.newBuilder()
                                                 .setHeader(Header.newBuilder()
                                                                  .setHeight(lastBlock.height() + 1)
                                                                  .setLastCheckpoint(checkpoint.height())
                                                                  .setLastReconfig(lastView.height())
                                                                  .setPrevious(lastBlock.hash.toByteString()))
                                                 .setBody(Body.newBuilder().setType(BodyType.USER))
                                                 .build())
                                  .build());
            bootstrapStore.put(block.hash, block.block);
            lastBlock = block;
        }

        HashedCertifiedBlock anchor = lastBlock;

        for (int i = 0; i < 5; i++) {
            HashedCertifiedBlock block = new HashedCertifiedBlock(
                    CertifiedBlock.newBuilder()
                                  .setBlock(Block.newBuilder()
                                                 .setHeader(Header.newBuilder()
                                                                  .setHeight(lastBlock.height() + 1)
                                                                  .setLastCheckpoint(checkpoint.height())
                                                                  .setLastReconfig(lastView.height())
                                                                  .setPrevious(lastBlock.hash.toByteString()))
                                                 .setBody(Body.newBuilder().setType(BodyType.USER))
                                                 .build())
                                  .build());
            bootstrapStore.put(block.hash, block.block);
            lastBlock = block;
        }

        final HashedCertifiedBlock lastCheckpoint = checkpoint;
        Member member = members.get(0);
        BootstrapClient client = mock(BootstrapClient.class);
        final HashedCertifiedBlock view = lastView;
        when(client.sync(any())).then(new Answer<>() {
            @Override
            public ListenableFuture<Initial> answer(InvocationOnMock invocation) throws Throwable {
                SettableFuture<Initial> futureSailor = SettableFuture.create();
                Synchronize rep = invocation.getArgumentAt(0, Synchronize.class);
                Builder initial = Initial.newBuilder()
                                         .setCheckpoint(lastCheckpoint.block)
                                         .setCheckpointView(view.block)
                                         .setGenesis(genesis.block);
                bootstrapStore.viewChainFrom(view.height(), 0).forEachRemaining(l -> {
                });
                futureSailor.set(initial.build());
                return futureSailor;
            }
        });
        when(client.fetchViewChain(any())).then(new Answer<>() {
            @Override
            public ListenableFuture<Blocks> answer(InvocationOnMock invocation) throws Throwable {
                SettableFuture<Blocks> futureSailor = SettableFuture.create();
                BlockReplication rep = invocation.getArgumentAt(0, BlockReplication.class);
                Blocks blocks = Blocks.newBuilder().build();
                futureSailor.set(blocks);
                return futureSailor;
            }
        });
        when(client.fetchBlocks(any())).then(new Answer<>() {
            @Override
            public ListenableFuture<Blocks> answer(InvocationOnMock invocation) throws Throwable {
                SettableFuture<Blocks> futureSailor = SettableFuture.create();
                BlockReplication rep = invocation.getArgumentAt(0, BlockReplication.class);
                Blocks blocks = Blocks.newBuilder().build();
                futureSailor.set(blocks);
                return futureSailor;
            }
        });

        @SuppressWarnings("unchecked")
        CommonCommunications<BootstrapClient, Service> comms = mock(CommonCommunications.class);
        when(comms.apply(any(), any())).thenReturn(client);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Duration duration = Duration.ofMillis(100);
        Store store = new Store(new MVStore.Builder().open());
        Bootstrapper boot = new Bootstrapper(anchor.block.getBlock(), member, context, comms, 0.15, store, 5, scheduler,
                100, duration, 100);
        CompletableFuture<Pair<HashedCertifiedBlock, HashedCertifiedBlock>> syncFuture = boot.synchronize();
        syncFuture.get(10, TimeUnit.SECONDS);
    }
}
