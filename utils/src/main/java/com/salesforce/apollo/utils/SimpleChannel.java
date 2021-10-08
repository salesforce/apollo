/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hal.hildebrand
 *
 */
public class SimpleChannel<T> implements Closeable, Channel<T> {
    static final Logger log = LoggerFactory.getLogger(SimpleChannel.class);

    private final AtomicBoolean    closed = new AtomicBoolean();
    private volatile Thread        handler;
    private final BlockingQueue<T> queue;
    private final String           label;

    public SimpleChannel(String label, BlockingQueue<T> queue) {
        this.queue = queue;
        this.label = label;
    }

    public SimpleChannel(String label, int capacity) {
        queue = new ArrayBlockingQueue<>(capacity);
        this.label = label;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (handler != null) {
            handler.interrupt();
            handler = null;
        }
    }

    @Override
    public void consume(Consumer<List<T>> consumer) {
        if (closed.get()) {
            log.debug("Channel is already closed");
            return;
        }
        if (handler != null) {
            throw new IllegalStateException("Handler already established");
        }
        handler = new Thread(() -> {
            while (!closed.getAcquire()) {
                List<T> available = new ArrayList<T>();
                T polled;
                try {
                    polled = queue.take();
                } catch (InterruptedException e) {
                    return;
                }
                if (closed.get()) {
                    return;
                }
                if (polled != null) {
                    int count = queue.size();
                    queue.drainTo(available, count);
                    available.add(0, polled);
                    try {
                        consumer.accept(available);
                    } catch (Throwable e) {
                        log.error("Error in consumer", e);
                    }
                }
            }
        }, label);
        handler.setDaemon(true);
        handler.start();
    }

    @Override
    public void consumeEach(Consumer<T> consumer) {
        consume(elements -> {
            for (T element : elements) {
                consumer.accept(element);
            }
        });
    }

    @Override
    public boolean offer(T element) {
        if (closed.get()) {
            return false;
        }
        return queue.offer(element);
    }

    @Override
    public void open() {
        if (!closed.compareAndSet(true, false)) {
            queue.clear();
        }
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public void submit(T element) {
        if (closed.get()) {
            return;
        }
        try {
            queue.put(element);
        } catch (InterruptedException e) {
//            log.warn("Interrupted in submit", e);
        }
    }
}
