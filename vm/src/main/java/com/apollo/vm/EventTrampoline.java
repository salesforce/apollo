/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.apollo.vm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;

/**
 * @author hal.hildebrand
 *
 */
public class EventTrampoline {
    public static class Event {
        public final Any    body;
        public final String discriminator;

        public Event(String discriminator, Any body) {
            this.discriminator = discriminator;
            this.body = body;
        }
    }

    private static final Logger                        log      = LoggerFactory.getLogger(EventTrampoline.class);
    private final Map<String, BiConsumer<String, Any>> handlers = new ConcurrentHashMap<>();
    private List<Event>                                pending  = new CopyOnWriteArrayList<>();

    public void evaluate() {
        try {
            for (Event event : pending) {
                BiConsumer<String, Any> handler = handlers.get(event.discriminator);
                if (handler != null) {
                    try {
                        handler.accept(event.discriminator, event.body);
                    } catch (Throwable e) {
                        log.trace("handler failed for {}", e);
                    }
                }
            }
        } finally {
            pending.clear();
        }
    }

    public void publish(Event event) {
        pending.add(event);
    }

    public void publish(String discriminator, Any body) {
        publish(new Event(discriminator, body));
    }

    public void register(String discriminator, BiConsumer<String, Any> handler) {
        handlers.put(discriminator, handler);
    }
}
