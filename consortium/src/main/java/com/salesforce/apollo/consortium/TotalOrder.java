/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.consortium.proto.ConsortiumMessage;
import com.salesforce.apollo.consortium.Consortium.Collaborator;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.messaging.Messenger.MessageChannelHandler.Msg;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class TotalOrder {
    public class Channel {
        private final HashKey                          id;
        private volatile int                           lastSequenceNumber = -1;
        private final PriorityQueue<ConsortiumMessage> queue;

        public Channel(HashKey id) {
            this.id = id;
            queue = new PriorityQueue<ConsortiumMessage>((a, b) -> {
                return Integer.compare(a.getSequenceNumber(), b.getSequenceNumber());
            });
        }

        public void clear() {
            lastSequenceNumber = -1;
            queue.clear();
        }

        public void enqueue(ConsortiumMessage msg) {
            queue.add(msg);
        }

        public ConsortiumMessage next() {
            ConsortiumMessage message = queue.peek();
            if (message == null) {
                return null;
            }
            final int current = lastSequenceNumber;
            if (message.getSequenceNumber() == current + 1) {
                message = queue.poll();
                assert message.getSequenceNumber() == current + 1 : "Kimpossible!";
                return message;
            }
            if (message.getSequenceNumber() <= current) {
                log.error("Protocol error!  Head of channel of {} is <= than the current sequence # {}", id, current);
            }
            return null;
        }
    }

    private static Logger                                log      = LoggerFactory.getLogger(TotalOrder.class);
    private final Map<HashKey, Channel>                  channels = new HashMap<>();
    private final BiConsumer<ConsortiumMessage, HashKey> processor;
    private final ReadWriteLock                          lock     = new ReentrantReadWriteLock(true);
    private final AtomicBoolean                          started  = new AtomicBoolean();

    public TotalOrder(BiConsumer<ConsortiumMessage, HashKey> processor, Context<Collaborator> context) {
        this.processor = processor;
        context.allMembers().forEach(m -> channels.put(m.getId(), new Channel(m.getId())));
        context.register(new MembershipListener<Consortium.Collaborator>() {

            @Override
            public void fail(Collaborator member) {
                if (!started.get()) {
                    return;
                }
                final Lock write = lock.writeLock();
                write.lock();
                try {
                    Channel channel = channels.get(member.getId());
                    if (channel == null) {
                        log.trace("Unknown member failed: {}", member.getId());
                    }
                    channel.clear();
                } finally {
                    write.unlock();
                }
            }

            @Override
            public void recover(Collaborator member) {
                // TODO Auto-generated method stub

            }
        });
    }

    public void process(Collection<Msg> msgs) {
        if (!started.get()) {
            return;
        }
        final Lock write = lock.writeLock();
        write.lock();
        try {
            msgs.forEach(m -> process(m));
            processHead();
        } finally {
            write.unlock();
        }
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
    }

    private void process(Msg m) {
        ConsortiumMessage message;
        try {
            message = ConsortiumMessage.parseFrom(m.content);
        } catch (InvalidProtocolBufferException e) {
            log.error("Corrupted message received from {}", m.from.getId(), e);
            return;
        }
        Channel channel = channels.get(m.from.getId());
        if (channel == null) {
            log.error("Message received from {} which is not a consortium member", m.from.getId());
            return;
        }
        channel.enqueue(message);
    }

    // Deliever all messages in sequence that are available
    private void processHead() {
        channels.forEach((id, channel) -> {
            ConsortiumMessage message = channel.next();
            if (message != null) {
                processor.accept(message, channel.id);
            }
        });
    }
}
