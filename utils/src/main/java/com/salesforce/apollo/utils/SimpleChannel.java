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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hal.hildebrand
 *
 */
public class SimpleChannel<T> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(SimpleChannel.class);

    private AtomicBoolean    closed = new AtomicBoolean();
    private Thread           handler;
    private BlockingQueue<T> queue;

    public SimpleChannel(int capacity) {
        queue = new LinkedBlockingDeque<>(capacity);
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

    public void consume(Consumer<List<T>> consumer) {
        if (closed.get()) {
            throw new IllegalStateException("Channel already closed");
        }
        if (handler != null) {
            throw new IllegalStateException("Handler already established");
        }
        handler = new Thread(() -> {
            while (!closed.get()) {
                try {
                    List<T> available = new ArrayList<T>();
                    var polled = queue.poll(1, TimeUnit.SECONDS);
                    if (polled != null) {
                        available.add(polled);
                        queue.drainTo(available);
                        try {
                            consumer.accept(available);
                        } catch (Throwable e) {
                            log.error("Error in consumer", e);
                        }
                    }
                } catch (InterruptedException e) {
                    return; // Normal exit
                }

            }
        }, "Consumer");
        handler.start();
    }

    public void consumeEach(Consumer<T> consumer) {
        if (closed.get()) {
            throw new IllegalStateException("Channel already closed");
        }
        if (handler != null) {
            throw new IllegalStateException("Handler already established");
        }
        handler = new Thread(() -> {
            while (!closed.get()) {
                try {
                    var polled = queue.poll(1, TimeUnit.SECONDS);
                    if (polled != null) {
                        try {
                            consumer.accept(polled);
                        } catch (Throwable e) {
                            log.error("Error in consumer", e);
                        }
                    }
                } catch (InterruptedException e) {
                    return; // Normal exit
                }

            }
        }, "Consumer");
        handler.start();
    }

    public int size() {
        return queue.size();
    }

    public boolean offer(T element) {
        return queue.offer(element);
    }

    public void submit(T element) {
        try {
            queue.put(element);
        } catch (InterruptedException e) {
            log.warn("Interrupted in submit", e);
        }
    }
}
