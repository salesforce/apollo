/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.comm;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.choam.proto.BlockReplication;
import com.salesfoce.apollo.choam.proto.Blocks;
import com.salesfoce.apollo.choam.proto.CheckpointReplication;
import com.salesfoce.apollo.choam.proto.CheckpointSegments;
import com.salesfoce.apollo.choam.proto.Initial;
import com.salesfoce.apollo.choam.proto.JoinRequest;
import com.salesfoce.apollo.choam.proto.SubmitTransaction;
import com.salesfoce.apollo.choam.proto.Synchronize;
import com.salesfoce.apollo.choam.proto.TerminalGrpc;
import com.salesfoce.apollo.choam.proto.TerminalGrpc.TerminalFutureStub;
import com.salesfoce.apollo.choam.proto.ViewMember;
import com.salesforce.apollo.choam.support.ChoamMetrics;
import com.salesforce.apollo.comm.ServerConnectionCache.CreateClientCommunications;
import com.salesforce.apollo.comm.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.membership.Member;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * @author hal.hildebrand
 *
 */
public class TerminalClient implements Terminal {

    public static CreateClientCommunications<Terminal> getCreate(ChoamMetrics metrics) {
        return (t, f, c) -> new TerminalClient(c, t, metrics);

    }

    private final ManagedServerConnection channel;

    private final TerminalFutureStub client;
    private final Member             member;
    @SuppressWarnings("unused")
    private final ChoamMetrics       metrics;

    public TerminalClient(ManagedServerConnection channel, Member member, ChoamMetrics metrics) {
        this.member = member;
        this.channel = channel;
        this.client = TerminalGrpc.newFutureStub(channel.channel).withCompression("gzip");
        this.metrics = metrics;
    }

    @Override
    public void close() {
        channel.release();
    }

    @Override
    public ListenableFuture<CheckpointSegments> fetch(CheckpointReplication request) {
        return client.fetch(request);
    }

    @Override
    public ListenableFuture<Blocks> fetchBlocks(BlockReplication replication) {
        return client.fetchBlocks(replication);
    }

    @Override
    public ListenableFuture<Blocks> fetchViewChain(BlockReplication replication) {
        return client.fetchViewChain(replication);
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public ListenableFuture<ViewMember> join(JoinRequest join) {
        return client.join(join);
    }

    public void release() {
        close();
    }

    @Override
    public ListenableFuture<Status> submit(SubmitTransaction request) {
        final var submit = client.submit(request);
        return new ListenableFuture<Status>() {

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return submit.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return submit.isCancelled();
            }

            @Override
            public boolean isDone() {
                return submit.isDone();
            }

            @Override
            public Status get() throws InterruptedException, ExecutionException {
                try {
                    final var result = submit.get();
                    return result != null ? Status.OK : null;
                } catch (ExecutionException e) {
                    final var cause = e.getCause();
                    if (cause instanceof StatusRuntimeException sre) {
                        return sre.getStatus();
                    }
                    throw e;
                }
            }

            @Override
            public Status get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
                                                           TimeoutException {
                try {
                    final var result = submit.get(timeout, unit);
                    return result != null ? Status.OK : null;
                } catch (TimeoutException e) {
                    return Status.UNAVAILABLE.withDescription("Server Timeout");
                } catch (ExecutionException e) {
                    final var cause = e.getCause();
                    if (cause instanceof StatusRuntimeException sre) {
                        return sre.getStatus();
                    }
                    throw e;
                }
            }

            @Override
            public void addListener(Runnable listener, Executor executor) {
                submit.addListener(listener, executor);
            }
        };
    }

    @Override
    public ListenableFuture<Initial> sync(Synchronize sync) {
        return client.sync(sync);
    }
}
