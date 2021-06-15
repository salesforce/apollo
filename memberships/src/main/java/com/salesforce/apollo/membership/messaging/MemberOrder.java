/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Context.MembershipListener;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger.MessageHandler.Msg;

/**
 * @author hal.hildebrand
 *
 */
public class MemberOrder {
    public class ActiveChannel implements Channel {
        private volatile int             flushTarget        = -1;
        private final Digest             id;
        private volatile int             lastSequenceNumber = -1;
        private final PriorityQueue<Msg> queue;

        public ActiveChannel(Digest id) {
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
//                log.trace("discarding previously seen: {}", msg.sequenceNumber);
            }
        }

        @Override
        public String toString() {
            return "AC[lsn=" + lastSequenceNumber + ", ft=" + flushTarget + "]";
        }

        @Override
        public Digest getId() {
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
                    log.trace("next msg: {} from: {} on: {}", message.sequenceNumber, message.from, member);
                    return message;
                } else if (message.sequenceNumber <= current) {
                    log.trace("discarding previously seen: {} <= {} on: {}", message.sequenceNumber, current, member);
                    queue.poll();
                    message = queue.peek();
                } else {
//                    log.trace("No Msg, next: {} head: {} flushTarget: {} on: {}", current, message.sequenceNumber,
//                              currentFlushTarget, member);
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

        Digest getId();

        default Msg next(int round) {
            return null;
        }

    }

    private static Logger                       log      = LoggerFactory.getLogger(MemberOrder.class);
    private final Map<Digest, Channel>          channels = new HashMap<>();
    private final Context<Member>               context;
    private final Lock                          lock     = new ReentrantLock(true);
    private final BiConsumer<Digest, List<Msg>> processor;
    private final AtomicBoolean                 started  = new AtomicBoolean();
    private final int                           ttl;
    private final int                           tick;
    private final Member                        member;

    @SuppressWarnings("unchecked")
    public MemberOrder(BiConsumer<Digest, List<Msg>> processor, Messenger messenger) {
        this.processor = processor;
        this.context = (Context<Member>) messenger.getContext();
        this.member = messenger.getMember();
        ttl = context.timeToLive();
        tick = Math.max(2, context.toleranceLevel() / 2);
        context.allMembers().forEach(m -> channels.put(m.getId(), new ActiveChannel(m.getId())));

        messenger.registerHandler((id, messages) -> process(messages, messenger.getRound()));
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
                        public Digest getId() {
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

    @Override
    public String toString() {
        return "MO[" + member.getId() + ", s=" + started + ", ttl=" + ttl + ", tick=" + tick + ", channels=" + channels
                + "]";
    }

    public void process(Collection<Msg> msgs, int round) {
        if (!started.get()) {
            return;
        }
        final Lock write = lock;
        write.lock();
        log.trace("processing {}", msgs);
        try {
            msgs.forEach(m -> {
                if (!started.get()) {
                    return;
                }
                process(m, round);
            });
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
        if (round % tick != 0) {
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

    private void deliver(List<Msg> msgs) {
        try {
            processor.accept(context.getId(), msgs);
        } catch (Throwable e) {
            log.error("Error processing messages  on: {}", member, e);
        }
    }

    private void flush(int round) {
        List<Msg> flushed = new ArrayList<>();
        int lastFlushed = -1;
        while (flushed.size() - lastFlushed > 0) {
            if (!started.get()) {
                return;
            }
            lastFlushed = flushed.size();
            for (Entry<Digest, Channel> e : channels.entrySet()) {
                Msg message = e.getValue().next(round);
                if (message != null) {
                    flushed.add(message);
                }
            }
        }
        if (flushed.size() != 0) {
            log.trace("Flushed: {} messages on: {}", flushed.size(), member);
            deliver(flushed);
        }
    }

    private void process(Msg m, int round) {
        Channel channel = channels.get(m.from.getId());
        if (channel == null) {
            log.trace("Message received on: {} from {} which is not a consortium member", member, m.from.getId());
            return;
        }
        channel.enqueue(m, round);
    }

    // Deliever all messages in sequence that are available
    private void processHead(int round) {
        List<Msg> delivered = new ArrayList<>();
        channels.forEach((id, channel) -> {
            if (!started.get()) {
                return;
            }
            Msg message = channel.next(round);
            if (message != null) {
                delivered.add(message);
            }
        });
        if (delivered.size() > 0) {
            log.trace("Delivering: {} messages on: {}", delivered.size(), member);
            deliver(delivered);
        }
    }
}
