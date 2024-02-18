package com.salesforce.apollo.archipelago.server;

import io.grpc.ServerCall;

import java.util.ArrayList;
import java.util.List;

// https://stackoverflow.com/a/53656689/181796
class DelayedServerCallListener<ReqT> extends ServerCall.Listener<ReqT> {
    private ServerCall.Listener<ReqT> delegate;
    private List<Runnable>            events = new ArrayList<>();

    @Override
    public synchronized void onCancel() {
        if (delegate == null) {
            events.add(() -> delegate.onCancel());
        } else {
            delegate.onCancel();
        }
    }

    @Override
    public synchronized void onComplete() {
        if (delegate == null) {
            events.add(() -> delegate.onComplete());
        } else {
            delegate.onComplete();
        }
    }

    @Override
    public synchronized void onHalfClose() {
        if (delegate == null) {
            events.add(() -> delegate.onHalfClose());
        } else {
            delegate.onHalfClose();
        }
    }

    @Override
    public synchronized void onMessage(ReqT message) {
        if (delegate == null) {
            events.add(() -> delegate.onMessage(message));
        } else {
            delegate.onMessage(message);
        }
    }

    @Override
    public synchronized void onReady() {
        if (delegate == null) {
            events.add(() -> delegate.onReady());
        } else {
            delegate.onReady();
        }
    }

    public synchronized void setDelegate(ServerCall.Listener<ReqT> delegate) {
        this.delegate = delegate;
        for (Runnable runnable : events) {
            runnable.run();
        }
        events = null;
    }
}
