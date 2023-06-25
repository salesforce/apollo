/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;

import org.h2.mvstore.MVStore;
import org.joou.ULong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.salesfoce.apollo.choam.proto.Checkpoint;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesfoce.apollo.choam.proto.Slice;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.choam.CHOAM;
import com.salesforce.apollo.choam.comm.Concierge;
import com.salesforce.apollo.choam.comm.Terminal;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.ContextImpl;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.stereotomy.ControlledIdentifierMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.mem.MemKERL;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class CheckpointAssemblerTest {

    private static final int CARDINALITY  = 10;
    private static final int SEGMENT_SIZE = 256;

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
        File checkpointDir = new File("target/checkpoint");
        Utils.clean(checkpointDir);
        checkpointDir.mkdirs();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        File chkptFile = new File(checkpointDir, "chkpt.chk");
        chkptFile.deleteOnExit();
        byte[] line = "aaaabbbdddasff;lkasdfa;sdlfkjasdf;lasdjfalsdfjas;dfkasdflasdkjfasd;kfasdlfjasdl;fkja;sdflasdkjfasdklf;asjfa;sfasdf;lkasjdfsa;flasj\n".getBytes();
        try (FileOutputStream os = new FileOutputStream(chkptFile); GZIPOutputStream gos = new GZIPOutputStream(os)) {
            for (int i = 0; i < 4096; i++) {
                gos.write(line);
                baos.write(line);
            }
            gos.close();
        }

        Context<Member> context = new ContextImpl<>(DigestAlgorithm.DEFAULT.getOrigin(), CARDINALITY, 0.2, 3);
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });
        var stereotomy = new StereotomyImpl(new MemKeyStore(), new MemKERL(DigestAlgorithm.DEFAULT), entropy);

        List<SigningMember> members = IntStream.range(0, CARDINALITY).mapToObj(i -> {
            try {
                return stereotomy.newIdentifier().get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException(e);
            }
        }).map(cpk -> new ControlledIdentifierMember(cpk)).map(e -> (SigningMember) e).toList();
        members.forEach(m -> context.activate(m));

        Checkpoint checkpoint = CHOAM.checkpoint(DigestAlgorithm.DEFAULT, chkptFile, SEGMENT_SIZE);

        SigningMember bootstrapping = members.get(0);

        Store store1 = new Store(DigestAlgorithm.DEFAULT, new MVStore.Builder().open());
        CheckpointState state = new CheckpointState(checkpoint,
                                                    store1.putCheckpoint(ULong.valueOf(0), chkptFile, checkpoint));

        File testFile = File.createTempFile("test-", "chkpt", checkpointDir);
        testFile.deleteOnExit();
        state.assemble(testFile);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        Digest originalHash = DigestAlgorithm.DEFAULT.digest(bais);
        Digest assembledHash;
        try (FileInputStream fis = new FileInputStream(testFile)) {
            assembledHash = DigestAlgorithm.DEFAULT.digest(fis);
        }

        assertEquals(originalHash, assembledHash);

        Terminal client = mock(Terminal.class);
        when(client.fetch(any())).then(new Answer<>() {
            @Override
            public ListenableFuture<CheckpointSegments> answer(InvocationOnMock invocation) throws Throwable {
                SettableFuture<CheckpointSegments> futureSailor = SettableFuture.create();
                CheckpointReplication rep = invocation.getArgument(0, CheckpointReplication.class);
                List<Slice> fetched = state.fetchSegments(BloomFilter.from(rep.getCheckpointSegments()), 2);
                System.out.println("Fetched: " + fetched.size());
                futureSailor.set(CheckpointSegments.newBuilder().addAllSegments(fetched).build());
                return futureSailor;
            }
        });
        @SuppressWarnings("unchecked")
        CommonCommunications<Terminal, Concierge> comm = mock(CommonCommunications.class);
        when(comm.connect(any())).thenReturn(client);

        Store store2 = new Store(DigestAlgorithm.DEFAULT, new MVStore.Builder().open());
        CheckpointAssembler boot = new CheckpointAssembler(Duration.ofMillis(10), ULong.valueOf(0), checkpoint,
                                                           bootstrapping, store2, comm, context, 0.00125,
                                                           DigestAlgorithm.DEFAULT);
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        assembled = boot.assemble(scheduler, Duration.ofMillis(10), r -> r.run());
        CheckpointState assembledCs;
        try {
            assembledCs = assembled.get(300, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            assembled.completeExceptionally(e);
            fail("Timeout waiting for assembly");
            return;
        }

        assertNotNull(assembledCs);

        // Recreate the checkpoint file
        File assembledFile = File.createTempFile("assembled-", "chkpt", checkpointDir);
        assembledCs.assemble(assembledFile);
    }
}
