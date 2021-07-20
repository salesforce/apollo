/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

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
        closed.set(true);
        if (handler != null) {
            handler.interrupt();
        }
    }

    public void consume(Consumer<List<T>> consumer) {
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

    public int size() {
        return queue.size();
    }

    public void submit(T element) {
        try {
            queue.put(element);
        } catch (InterruptedException e) {
            log.warn("Interrupted in submit", e);
        }
    }
}
