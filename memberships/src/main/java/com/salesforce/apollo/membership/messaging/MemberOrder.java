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
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
public class MemberOrder {
    public class ActiveChannel implements Channel {
        private volatile int             flushTarget        = -1;
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
        public void enqueue(Msg msg, int round) {
            final int last = lastSequenceNumber;
            if (msg.sequenceNumber > last) {
                final int currentFlushTarget = flushTarget;
                if (currentFlushTarget < 0) {
                    flushTarget = round + ttl;
                }
                queue.add(msg);
            } else {
                log.trace("discarding previously seen: {}", msg.sequenceNumber);
            }
        }

        @Override
        public HashKey getId() {
            return id;
        }

        @Override
        public Msg next(int round) {
            Msg message = queue.peek();
            final int current = lastSequenceNumber;
            final int currentFlushTarget = flushTarget;
            while (message != null) {
                if (message.sequenceNumber == current + 1 || ((currentFlushTarget > 0 && currentFlushTarget < round)
                        && message.sequenceNumber > current)) {
                    flushTarget = -1;
                    message = queue.remove();
                    lastSequenceNumber = message.sequenceNumber;
                    log.trace("next: {}:{}", message.from, message.sequenceNumber);
                    return message;
                } else if (message.sequenceNumber <= current) {
                    log.trace("discarding previously seen: {} <= {}", message.sequenceNumber, current);
                    queue.poll();
                    message = queue.peek();
                } else {
                    log.trace("No Msg, next: {} head: {} flushTarget: {}", current, message.sequenceNumber, currentFlushTarget);
                    return null;
                }
            }
            return null;
        }

    }

    public interface Channel {

        default void clear() {
        }

        default void enqueue(Msg msg, int round) {
        }

        HashKey getId();

        default Msg next(int round) {
            return null;
        }

    }

    private static Logger                  log      = LoggerFactory.getLogger(MemberOrder.class);
    private final Map<HashKey, Channel>    channels = new HashMap<>();
    private final Context<Member>          context;
    private final Lock                     lock     = new ReentrantLock(true);
    private final BiConsumer<Msg, HashKey> processor;
    private final AtomicBoolean            started  = new AtomicBoolean();
    private final int                      ttl;

    @SuppressWarnings("unchecked")
    public MemberOrder(BiConsumer<Msg, HashKey> processor, Messenger messenger) {
        this.processor = processor;
        this.context = (Context<Member>) messenger.getContext();
        ttl = context.timeToLive();
        context.allMembers().forEach(m -> channels.put(m.getId(), new ActiveChannel(m.getId())));

        messenger.registerHandler(messages -> process(messages, messenger.getRound()));
        messenger.register(round -> tick(round));
        context.register(new MembershipListener<Member>() {

            @Override
            public void fail(Member member) {
                if (!started.get()) {
                    return;
                }
                final Lock write = lock;
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
                final Lock write = lock;
                write.lock();
                try {
                    channels.put(member.getId(), new ActiveChannel(member.getId()));
                } finally {
                    write.unlock();
                }
            }
        });
    }

    public void process(Collection<Msg> msgs, int round) {
        if (!started.get()) {
            return;
        }
        final Lock write = lock;
        write.lock();
        log.trace("processing {}", msgs);
        try {
            msgs.forEach(m -> process(m, round));
            processHead(round);
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

    public void tick(int round) {
        if (round % ttl != 1) { // ttl + 1
            return;
        }
        final Lock write = lock;
        write.lock();
        try {
            flush(round);
        } finally {
            write.unlock();
        }
    }

    private void flush(int round) {
        log.trace("flushing");
        int flushed = 0;
        int lastFlushed = -1;
        while (flushed - lastFlushed > 0) {
            lastFlushed = flushed;
            for (Entry<HashKey, Channel> e : channels.entrySet()) {
                Msg message = e.getValue().next(round);
                if (message != null) {
                    processor.accept(message, e.getKey());
                    flushed++;
                }
            }
        }
    }

    private void process(Msg m, int round) {
        Channel channel = channels.get(m.from.getId());
        if (channel == null) {
            log.trace("Message received from {} which is not a consortium member", m.from.getId());
            return;
        }
        channel.enqueue(m, round);
    }

    // Deliever all messages in sequence that are available
    private void processHead(int round) {
        AtomicInteger delivered = new AtomicInteger();
        channels.forEach((id, channel) -> {
            Msg message = channel.next(round);
            if (message != null) {
                processor.accept(message, channel.getId());
                delivered.incrementAndGet();
            }
        });
        log.trace("Delivered: {} messages", delivered.get());
    }
}
