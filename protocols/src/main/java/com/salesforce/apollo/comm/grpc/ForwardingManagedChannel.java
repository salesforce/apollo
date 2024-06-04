//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.salesforce.apollo.comm.grpc;

import com.google.common.base.MoreObjects;
import io.grpc.*;

import java.util.concurrent.TimeUnit;

public class ForwardingManagedChannel extends ManagedChannel {
    private final ManagedChannel delegate;

    ForwardingManagedChannel(ManagedChannel delegate) {
        this.delegate = delegate;
    }

    public String authority() {
        return this.delegate.authority();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return this.delegate.awaitTermination(timeout, unit);
    }

    public void enterIdle() {
        this.delegate.enterIdle();
    }

    public ConnectivityState getState(boolean requestConnection) {
        return this.delegate.getState(requestConnection);
    }

    public boolean isShutdown() {
        return this.delegate.isShutdown();
    }

    public boolean isTerminated() {
        return this.delegate.isTerminated();
    }

    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
    MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        return this.delegate.newCall(methodDescriptor, callOptions);
    }

    public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
        this.delegate.notifyWhenStateChanged(source, callback);
    }

    public void resetConnectBackoff() {
        this.delegate.resetConnectBackoff();
    }

    public ManagedChannel shutdown() {
        return this.delegate.shutdown();
    }

    public ManagedChannel shutdownNow() {
        return this.delegate.shutdownNow();
    }

    public String toString() {
        return MoreObjects.toStringHelper(this).add("delegate", this.delegate).toString();
    }
}
