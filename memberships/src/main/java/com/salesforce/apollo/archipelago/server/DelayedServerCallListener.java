package com.salesforce.apollo.archipelago.server;

import io.grpc.ServerCall;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// https://stackoverflow.com/a/53656689/181796
class DelayedServerCallListener<ReqT> extends ServerCall.Listener<ReqT> {
    private final Lock                      lock   = new ReentrantLock();
    private       ServerCall.Listener<ReqT> delegate;
    private       List<Runnable>            events = new ArrayList<>();

    @Override
    public void onCancel() {
        lock.lock();
        try {
            if (delegate == null) {
                events.add(() -> delegate.onCancel());
            } else {
                delegate.onCancel();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onComplete() {
        lock.lock();
        try {
            if (delegate == null) {
                events.add(() -> delegate.onComplete());
            } else {
                delegate.onComplete();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onHalfClose() {
        lock.lock();
        try {
            if (delegate == null) {
                events.add(() -> delegate.onHalfClose());
            } else {
                delegate.onHalfClose();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onMessage(ReqT message) {
        lock.lock();
        try {
            if (delegate == null) {
                events.add(() -> delegate.onMessage(message));
            } else {
                delegate.onMessage(message);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onReady() {
        lock.lock();
        try {
            if (delegate == null) {
                events.add(() -> delegate.onReady());
            } else {
                delegate.onReady();
            }
        } finally {
            lock.unlock();
        }
    }

    public void setDelegate(ServerCall.Listener<ReqT> delegate) {
        lock.lock();
        try {
            this.delegate = delegate;
            for (Runnable runnable : events) {
                runnable.run();
            }
            events = null;
        } finally {
            lock.unlock();
        }
    }
}
