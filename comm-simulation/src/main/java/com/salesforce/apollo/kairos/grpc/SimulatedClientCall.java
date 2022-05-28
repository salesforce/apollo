/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.kairos.grpc;

import io.grpc.Attributes;
import io.grpc.ClientCall;
import io.grpc.Metadata;

/**
 * @author hal.hildebrand
 *
 */
abstract public class SimulatedClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {
    protected final ClientCall<ReqT, RespT> delegate;

    public SimulatedClientCall(ClientCall<ReqT, RespT> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void cancel(String message, Throwable cause) {
        delegate.cancel(message, cause);
    }

    @Override
    public Attributes getAttributes() {
        return delegate.getAttributes();
    }

    @Override
    public void halfClose() {
        delegate.halfClose();
    }

    @Override
    public boolean isReady() {
        return delegate.isReady();
    }

    @Override
    public void request(int numMessages) {
        scheduleRequest(numMessages);
    }

    @Override
    public void sendMessage(ReqT message) {
        scheduleMessage(message);
    }

    @Override
    public void setMessageCompression(boolean enabled) {
        delegate.setMessageCompression(enabled);
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
        scheduleStart(responseListener, headers);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    protected abstract void scheduleMessage(ReqT message);

    protected abstract void scheduleRequest(int numMessages);

    protected abstract void scheduleStart(Listener<RespT> responseListener, Metadata headers);

}
