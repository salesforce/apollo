/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.vm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apollo.vm.Trampoline;
import com.apollo.vm.Trampoline.Event;
import com.google.protobuf.Any;

/**
 * @author hal.hildebrand
 *
 */
public class VirtualMachine {
    private static class EventTrampoline {

        private final Map<String, BiConsumer<String, Any>> handlers = new ConcurrentHashMap<>();
        private List<Event>                                pending  = new CopyOnWriteArrayList<>();

        public void deregister(String discriminator) {
            handlers.remove(discriminator);
        }

        private void evaluate() {
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

        private void publish(Event event) {
            pending.add(event);
        }

        private void publish(String discriminator, Any body) {
            publish(new Event(discriminator, body));
        }

        private void register(String discriminator, BiConsumer<String, Any> handler) {
            handlers.put(discriminator, handler);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(VirtualMachine.class);
    private final Registry      registry;
    private final Trampoline    trampoline;

    public VirtualMachine() {
        EventTrampoline boundary = new EventTrampoline();
        trampoline = new Trampoline() {

            @Override
            public void publish(Event event) {
                boundary.publish(event);
            }

            @Override
            public void publish(String discriminator, Any body) {
                boundary.publish(discriminator, body);
            }
        };

        registry = new Registry() {

            @Override
            public void deregister(String discriminator) {
                boundary.deregister(discriminator);
            }

            @Override
            public void register(String discriminator, BiConsumer<String, Any> handler) {
                boundary.register(discriminator, handler);
            }
        };
    }

}
