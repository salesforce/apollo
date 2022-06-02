/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.h2.mvstore.MVStore;
import org.joou.ULong;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.SettableFuture;
import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesforce.apollo.choam.Parameters;
import com.salesforce.apollo.choam.Parameters.RuntimeParameters;
import com.salesforce.apollo.choam.TestChain;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.choam.support.Bootstrapper.SynchronizedState;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class BootstrapperTest {
    private static final int CARDINALITY = 10;

    @SuppressWarnings("preview")
    @Test
    public void smoke() throws Exception {
        var exec = Executors.newVirtualThreadPerTaskExecutor();
        var scheduler = Executors.newScheduledThreadPool(CARDINALITY, Thread.ofVirtual().factory());
        Context<SigningMember> context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), CARDINALITY, 0.2, 3);

        Store bootstrapStore = new Store(DigestAlgorithm.DEFAULT, new MVStore.Builder().open());
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        List<SigningMember> members = IntStream.range(0, CARDINALITY)
                                               .mapToObj(i -> stereotomy.newIdentifier().get())
                                               .map(cpk -> new ControlledIdentifierMember(cpk))
                                               .map(e -> (SigningMember) e)
                                               .toList();
        context.activate(members);
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

        bootstrapStore.validate(lastBlock.height(), ULong.valueOf(0));
        bootstrapStore.validateViewChain(testChain.getSynchronizeView().height());

        SigningMember member = members.get(0);

        @SuppressWarnings("unchecked")
        CommonCommunications<Terminal, Concierge> comms = mock(CommonCommunications.class);
        when(comms.apply(any(), same(member))).thenAnswer(invoke -> {
            Member to = invoke.getArgument(0, Member.class);
            return mockClient(to, bootstrapStore, testChain);
        });
        Store store = new Store(DigestAlgorithm.DEFAULT, new MVStore.Builder().open());

        Bootstrapper boot = new Bootstrapper(testChain.getAnchor(),
                                             Parameters.newBuilder()
                                                       .setGossipDuration(Duration.ofMillis(10))
                                                       .build(RuntimeParameters.newBuilder()
                                                                               .setContext(context)
                                                                               .setMember(member)
                                                                               .setExec(exec)
                                                                               .setScheduler(scheduler)
                                                                               .build()),
                                             store, comms);

        CompletableFuture<SynchronizedState> syncFuture = boot.synchronize();
        SynchronizedState state = syncFuture.get(10, TimeUnit.SECONDS);
        assertNotNull(state);
        assertNotNull(state.genesis);
        assertNotNull(state.checkpoint);
        assertNotNull(state.lastCheckpoint);
        assertNotNull(state.lastView);
    }

    private Terminal mockClient(Member to, Store bootstrapStore, TestChain testChain) {
        Terminal client = mock(Terminal.class);
        when(client.getMember()).thenReturn(to);

        when(client.sync(any())).then(invocation -> {
            SettableFuture<Initial> futureSailor = SettableFuture.create();
            Initial.Builder initial = Initial.newBuilder()
                                             .setCheckpoint(testChain.getSynchronizeCheckpoint().certifiedBlock)
                                             .setCheckpointView(testChain.getSynchronizeView().certifiedBlock)
                                             .setGenesis(testChain.getGenesis().certifiedBlock);
            futureSailor.set(initial.build());
            return futureSailor;
        });
        when(client.fetchViewChain(any())).then(invocation -> {
            SettableFuture<Blocks> futureSailor = SettableFuture.create();
            BlockReplication rep = invocation.getArgument(0, BlockReplication.class);
            BloomFilter<ULong> bff = BloomFilter.from(rep.getBlocksBff());
            Blocks.Builder blocks = Blocks.newBuilder();
            bootstrapStore.fetchViewChain(bff, blocks, 1, ULong.valueOf(rep.getFrom()), ULong.valueOf(rep.getTo()));
            futureSailor.set(blocks.build());
            return futureSailor;
        });
        when(client.fetchBlocks(any())).then(invocation -> {
            SettableFuture<Blocks> futureSailor = SettableFuture.create();
            BlockReplication rep = invocation.getArgument(0, BlockReplication.class);
            BloomFilter<ULong> bff = BloomFilter.from(rep.getBlocksBff());
            Blocks.Builder blocks = Blocks.newBuilder();
            bootstrapStore.fetchBlocks(bff, blocks, 5, ULong.valueOf(rep.getFrom()), ULong.valueOf(rep.getTo()));
            futureSailor.set(blocks.build());
            return futureSailor;
        });
        return client;
    }

}
