/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hal.hildebrand
 *
 */
public class ChannelConsumer<T> {
    private static final Logger log = LoggerFactory.getLogger(ChannelConsumer.class);

    private AtomicBoolean          closed = new AtomicBoolean();
    private Thread                 handler;
    private final BlockingQueue<T> queue;

    public BlockingQueue<T> getChannel() {
        return queue;
    }

    public ChannelConsumer(BlockingQueue<T> queue) {
        this.queue = queue;
    }

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
            throw new IllegalStateException("Consumer already closed");
        }
        if (handler != null) {
            throw new IllegalStateException("Handler already established");
        }
        handler = new Thread(() -> {
            while (!closed.get()) {
                try {
                    List<T> available = new ArrayList<T>();
                    var polled = queue.take();
                    if (polled != null) {
                        queue.drainTo(available);
                        available.add(0, polled);
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
        }, "ChannelConsumer");
        handler.setDaemon(true);
        handler.start();
    }

    public void consumeEach(Consumer<T> consumer) {
        consume(elements -> {
            for (T element : elements) {
                consumer.accept(element);
            }
        });
    }
}
