/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.archipeligo;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;

import java.util.concurrent.TimeUnit;

import com.google.common.base.MoreObjects;
import com.salesforce.apollo.archipeligo.ServerConnectionCache.ManagedServerConnection;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

public class ReleasableManagedChannel<To extends Member> extends ManagedChannel {

    private final Digest                      context;
    private final ManagedServerConnection<To> delegate;

    ReleasableManagedChannel(Digest context, ManagedServerConnection<To> delegate) {
        this.context = context;
        this.delegate = delegate;
    }

    @Override
    public String authority() {
        return delegate.getChannel().authority();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.getChannel().awaitTermination(timeout, unit);
    }

    @Override
    public void enterIdle() {
        delegate.getChannel().enterIdle();
    }

    @Override
    public ConnectivityState getState(boolean requestConnection) {
        return delegate.getChannel().getState(requestConnection);
    }

    public To getTo() {
        return delegate.getTo();
    }

    @Override
    public boolean isShutdown() {
        return delegate.getChannel().isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.getChannel().isTerminated();
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                                                                         CallOptions callOptions) {
        return new SimpleForwardingClientCall<RequestT, ResponseT>(delegate.getChannel()
                                                                           .newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(Listener<ResponseT> responseListener, Metadata headers) {
                headers.put(Router.CONTEXT_METADATA_KEY, qb64(context));
                headers.put(Router.TARGET_METADATA_KEY, qb64(delegate.getTo().getId()));
                super.start(responseListener, headers);
            }
        };
    }

    @Override
    public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
        delegate.getChannel().notifyWhenStateChanged(source, callback);
    }

    public void release() {
        delegate.release();
    }

    @Override
    public void resetConnectBackoff() {
        delegate.getChannel().resetConnectBackoff();
    }

    @Override
    public ManagedChannel shutdown() {
        return delegate.getChannel().shutdown();
    }

    @Override
    public ManagedChannel shutdownNow() {
        return delegate.getChannel().shutdownNow();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("delegate", delegate).toString();
    }
}
