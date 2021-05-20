/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import static com.salesforce.apollo.test.pregen.PregenPopulation.getMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.math3.random.BitsStreamGenerator;
import org.apache.commons.math3.random.MersenneTwister;
import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.salesfoce.apollo.consortium.proto.Checkpoint;
import com.salesfoce.apollo.consortium.proto.CheckpointReplication;
import com.salesfoce.apollo.consortium.proto.CheckpointSegments;
import com.salesfoce.apollo.consortium.proto.Slice;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.consortium.Consortium.BootstrappingService;
import com.salesforce.apollo.consortium.comms.BootstrapClient;
import com.salesforce.apollo.consortium.support.CheckpointAssembler;
import com.salesforce.apollo.consortium.support.CheckpointState;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.HashKey;
import com.salesforce.apollo.protocols.Utils;

import io.github.olivierlemasle.ca.CertificateWithPrivateKey;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointAssemblerTest {

    private static final int                               BLOCK_SIZE = 256;
    private static Map<HashKey, CertificateWithPrivateKey> certs;

    @BeforeAll
    public static void beforeClass() {
        certs = IntStream.range(1, 11)
                         .parallel()
                         .mapToObj(i -> getMember(i))
                         .collect(Collectors.toMap(cert -> Utils.getMemberId(cert.getX509Certificate()), cert -> cert));
    }

    private CompletableFuture<CheckpointState> assembled;

    @AfterEach
    public void after() {
        if (assembled != null) {
            assembled.completeExceptionally(new TimeoutException());
            assembled = null;
        }
    }

    @Test
    public void functional() throws Exception {
        BitsStreamGenerator entropy = new MersenneTwister(0x1638);
        File checkpointDir = new File("target/checkpoint");
        Utils.clean(checkpointDir);
        checkpointDir.mkdirs();

        File chkptFile = new File(checkpointDir, "chkpt.chk");
        try (FileOutputStream os = new FileOutputStream(chkptFile)) {
            for (int i = 0; i < 1024; i++) {
                os.write("aaaabbbdddasff;lkasdfa;sdlfkjasdf;lasdjfalsdfjas;dfkasdflasdkjfasd;kfasdlfjasdl;fkja;sdflasdkjfasdklf;asjfa;sfasdf;lkasjdfsa;flasj\n".getBytes());
            }
        }

        Context<Member> context = new Context<>(HashKey.ORIGIN);
        List<Member> members = certs.values()
                                    .stream()
                                    .map(c -> new Member(c.getX509Certificate()))
                                    .peek(m -> context.activate(m))
                                    .collect(Collectors.toList());

        Checkpoint checkpoint = CollaboratorContext.checkpoint(chkptFile, BLOCK_SIZE);

        Member bootstrapping = members.get(0);

        Store store1 = new Store(new MVStore.Builder().open());
        CheckpointState state = new CheckpointState(checkpoint, store1.putCheckpoint(0, chkptFile, checkpoint));

        // Check that we can assemble the checkpoint file and create a new checkpoint
        // from that test file and generate an identical checkpoint
        File testFile = File.createTempFile("test-", "chkpt", checkpointDir);
        state.assemble(testFile);

        assertEquals(chkptFile.length(), testFile.length());
        Checkpoint testCs = CollaboratorContext.checkpoint(testFile, BLOCK_SIZE);
        assertEquals(checkpoint.getSegmentsCount(), testCs.getSegmentsCount());
        for (int i = 0; i < checkpoint.getSegmentsCount(); i++) {
            assertEquals(new HashKey(checkpoint.getSegments(i)), new HashKey(testCs.getSegments(i)),
                         "Segment: " + i + " does not match");
        }
        assertEquals(new HashKey(checkpoint.getStateHash()), new HashKey(testCs.getStateHash()));

        BootstrapClient client = mock(BootstrapClient.class);
        when(client.fetch(any())).then(new Answer<>() {
            @Override
            public ListenableFuture<CheckpointSegments> answer(InvocationOnMock invocation) throws Throwable {
                SettableFuture<CheckpointSegments> futureSailor = SettableFuture.create();
                CheckpointReplication rep = invocation.getArgumentAt(0, CheckpointReplication.class);
                List<Slice> fetched = state.fetchSegments(BloomFilter.from(rep.getCheckpointSegments()), 10, entropy);
                System.out.println("Fetched: " + fetched.size());
                futureSailor.set(CheckpointSegments.newBuilder().addAllSegments(fetched).build());
                return futureSailor;
            }
        });
        @SuppressWarnings("unchecked")
        CommonCommunications<BootstrapClient, BootstrappingService> comm = mock(CommonCommunications.class);
        when(comm.apply(any(), any())).thenReturn(client);

        Store store2 = new Store(new MVStore.Builder().open());
        CheckpointAssembler boot = new CheckpointAssembler(0, checkpoint, bootstrapping, store2, comm, context, 0.125);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        assembled = boot.assemble(scheduler, Duration.ofMillis(10));
        CheckpointState assembledCs;
        try {
            assembledCs = assembled.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            assembled.completeExceptionally(e);
            fail("Timeout waiting for assembly");
            return;
        }

        assertNotNull(assembledCs);

        // Recreate the checkpoint file
        File assembledFile = File.createTempFile("assembled-", "chkpt", checkpointDir);
        assembledCs.assemble(assembledFile);

        assertEquals(chkptFile.length(), assembledFile.length());

        // create a checkpoint from the assembled file
        Checkpoint assembledCheckpoint = CollaboratorContext.checkpoint(assembledFile, BLOCK_SIZE);

        // same segment count
        assertEquals(checkpoint.getSegmentsCount(), assembledCheckpoint.getSegmentsCount());

        // same hash
        assertEquals(new HashKey(checkpoint.getStateHash()), new HashKey(assembledCheckpoint.getStateHash()));
    }
}
