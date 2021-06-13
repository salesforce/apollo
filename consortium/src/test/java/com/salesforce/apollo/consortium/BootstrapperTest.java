/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
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
import com.salesforce.apollo.consortium.Consortium.BootstrappingService;
import com.salesforce.apollo.consortium.comms.BootstrapClient;
import com.salesforce.apollo.consortium.support.Bootstrapper;
import com.salesforce.apollo.consortium.support.Bootstrapper.SynchronizedState;
import com.salesforce.apollo.consortium.support.HashedCertifiedBlock;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.cert.CertificateWithPrivateKey;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.impl.SigningMemberImpl;
import com.salesforce.apollo.utils.BloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class BootstrapperTest {
    private static Map<Digest, CertificateWithPrivateKey> certs;

    private static final int CARDINALITY = 10;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(0, CARDINALITY)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Member.getMemberIdentifier(cert.getX509Certificate()),
                                                   cert -> cert));
    }

    @Test
    public void smoke() throws Exception {
        Context<Member> context = new Context<>(DigestAlgorithm.DEFAULT.getOrigin(), 3);

        Store bootstrapStore = new Store(DigestAlgorithm.DEFAULT, new MVStore.Builder().open());

        List<SigningMember> members = certs.values()
                                           .stream()
                                           .map(c -> new SigningMemberImpl(
                                                   Member.getMemberIdentifier(c.getX509Certificate()),
                                                   c.getX509Certificate(), c.getPrivateKey(),
                                                   new Signer(0, c.getPrivateKey()),
                                                   c.getX509Certificate().getPublicKey()))
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

        SigningMember member = members.get(0);
        BootstrapClient client = mock(BootstrapClient.class);

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
        CommonCommunications<BootstrapClient, BootstrappingService> comms = mock(CommonCommunications.class);
        when(comms.apply(any(), any())).thenReturn(client);
        Store store = new Store(DigestAlgorithm.DEFAULT, new MVStore.Builder().open());

        Bootstrapper boot = new Bootstrapper(testChain.getAnchor(),
                Parameters.newBuilder()
                          .setMsgParameters(com.salesforce.apollo.membership.messaging.Messenger.Parameters.newBuilder()
                                                                                                           .build())
                          .setContext(context)
                          .setMember(member)
                          .setSynchonrizeDuration(Duration.ofMillis(100))
                          .setScheduler(Executors.newSingleThreadScheduledExecutor())
                          .build(),
                store, comms);

        CompletableFuture<SynchronizedState> syncFuture = boot.synchronize();
        SynchronizedState state = syncFuture.get(10, TimeUnit.SECONDS);
        assertNotNull(state);
        assertNotNull(state.genesis);
        assertNotNull(state.checkpoint);
        assertNotNull(state.lastCheckpoint);
        assertNotNull(state.lastView);
    }

}
