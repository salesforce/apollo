/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

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

import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger.MessageChannelHandler.Msg;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class TotalOrder {
    public class ActiveChannel implements Channel {
        private final HashKey            id;
        private volatile int             lastSequenceNumber = -1;
        private final PriorityQueue<Msg> queue;

        public ActiveChannel(HashKey id) {
            this.id = id;
            queue = new PriorityQueue<Msg>((a, b) -> {
                return Integer.compare(a.sequenceNumber, b.sequenceNumber);
            });
        }

        @Override
        public void clear() {
            lastSequenceNumber = -1;
            queue.clear();
        }

        @Override
        public void enqueue(Msg msg) {
            queue.add(msg);
        }

        @Override
        public HashKey getId() {
            return id;
        }

        @Override
        public Msg next() {
            Msg message = queue.peek();
            if (message == null) {
                log.trace("No msgs in queue");
                return null;
            }
            final int current = lastSequenceNumber;
            if (message.sequenceNumber == current + 1) {
                message = queue.poll();
                assert message.sequenceNumber == current + 1 : "Kimpossible!";
                lastSequenceNumber = message.sequenceNumber;
                return message;
            }
            if (message.sequenceNumber <= current) {
                log.error("Protocol error!  Head of channel of {} is <= than the current sequence # {}", id, current);
            } else {
                log.trace("Next: {} head: {}", current, message.sequenceNumber);
            }
            return null;
        }
    }

    public interface Channel {

        default void clear() {
        }

        default void enqueue(Msg msg) {
        }

        HashKey getId();

        default Msg next() {
            return null;
        }

    }

    private static Logger                  log      = LoggerFactory.getLogger(TotalOrder.class);
    private final Map<HashKey, Channel>    channels = new HashMap<>();
    private final Context<Member>          context;
    private final ReadWriteLock            lock     = new ReentrantReadWriteLock(true);
    private final BiConsumer<Msg, HashKey> processor;
    private final AtomicBoolean            started  = new AtomicBoolean();

    @SuppressWarnings("unchecked")
    public TotalOrder(BiConsumer<Msg, HashKey> processor, Context<? extends Member> ctx) {
        this.processor = processor;
        this.context = (Context<Member>) ctx;
        context.allMembers().forEach(m -> channels.put(m.getId(), new ActiveChannel(m.getId())));
        context.register(new MembershipListener<Member>() {

            @Override
            public void fail(Member member) {
                if (!started.get()) {
                    return;
                }
                final Lock write = lock.writeLock();
                write.lock();
                try {
                    Channel channel = channels.put(member.getId(), new Channel() {
                        @Override
                        public HashKey getId() {
                            return member.getId();
                        }
                    });
                    if (channel == null) {
                        log.trace("Unknown member failed: {}", member.getId());
                    }
                    channel.clear();
                } finally {
                    write.unlock();
                }
            }

            @Override
            public void recover(Member member) {
                if (!started.get()) {
                    return;
                }
                final Lock write = lock.writeLock();
                write.lock();
                try {
                    channels.put(member.getId(), new ActiveChannel(member.getId()));
                } finally {
                    write.unlock();
                }
            }
        });
    }

    public void process(Collection<Msg> msgs) {
        if (!started.get()) {
            return;
        }
        final Lock write = lock.writeLock();
        write.lock();
        log.trace("processing {}", msgs);
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
        Channel channel = channels.get(m.from.getId());
        if (channel == null) {
            log.trace("Message received from {} which is not a consortium member", m.from.getId());
            return;
        }
        channel.enqueue(m);
    }

    // Deliever all messages in sequence that are available
    private void processHead() {
        channels.forEach((id, channel) -> {
            Msg message = channel.next();
            if (message != null) {
                processor.accept(message, channel.getId());
            }
        });
    }
}
