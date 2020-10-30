/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static com.salesforce.apollo.membership.messaging.communications.MessagingClientCommunications.getCreate;
import static java.util.concurrent.ForkJoinPool.commonPool;

import java.security.SecureRandom;
import java.security.Signature;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.salesfoce.apollo.proto.Message;
import com.salesfoce.apollo.proto.MessageBff;
import com.salesfoce.apollo.proto.Messages;
import com.salesfoce.apollo.proto.Push;
import com.salesfoce.apollo.proto.Push.Builder;
import com.salesforce.apollo.comm.CommonCommunications;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.Messenger.MessageChannelHandler.Msg;
import com.salesforce.apollo.membership.messaging.communications.MessagingClientCommunications;
import com.salesforce.apollo.membership.messaging.communications.MessagingServerCommunications;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Messenger {

    @FunctionalInterface
    public interface MessageChannelHandler {
        class Msg {
            public final int    channel;
            public final byte[] content;
            public final Member from;

            public Msg(Member from, int channel, byte[] content) {
                this.channel = channel;
                this.from = from;
                this.content = content;
            }
        }

        void message(List<Msg> messages);
    }

    public static class Parameters {
        public static class Builder {

            private SecureRandom        entropy;
            private int                 falsePositiveRate;
            private long                frequency;
            private HashKey             id;
            private Member              member;
            private MessagingMetrics    metrics;
            private Supplier<Signature> signature;
            private TimeUnit            timeUnit;

            public Parameters build() {
                return new Parameters(id, member, signature, frequency, timeUnit, falsePositiveRate, entropy, metrics);
            }

            public SecureRandom getEntropy() {
                return entropy;
            }

            public int getFalsePositiveRate() {
                return falsePositiveRate;
            }

            public long getFrequency() {
                return frequency;
            }

            public HashKey getId() {
                return id;
            }

            public Member getMember() {
                return member;
            }

            public MessagingMetrics getMetrics() {
                return metrics;
            }

            public Supplier<Signature> getSignature() {
                return signature;
            }

            public TimeUnit getTimeUnit() {
                return timeUnit;
            }

            public Builder setEntropy(SecureRandom entropy) {
                this.entropy = entropy;
                return this;
            }

            public Builder setFalsePositiveRate(int falsePositiveRate) {
                this.falsePositiveRate = falsePositiveRate;
                return this;
            }

            public Builder setFrequency(long frequency) {
                this.frequency = frequency;
                return this;
            }

            public Builder setId(HashKey id) {
                this.id = id;
                return this;
            }

            public Builder setMember(Member member) {
                this.member = member;
                return this;
            }

            public Builder setMetrics(MessagingMetrics metrics) {
                this.metrics = metrics;
                return this;
            }

            public Builder setSignature(Supplier<Signature> signature) {
                this.signature = signature;
                return this;
            }

            public Builder setTimeUnit(TimeUnit timeUnit) {
                this.timeUnit = timeUnit;
                return this;
            }

        }

        public final SecureRandom        entropy;
        public final int                 falsePositiveRate;
        public final long                frequency;
        public final HashKey             id;
        public final Member              member;
        public final MessagingMetrics    metrics;
        public final Supplier<Signature> signature;
        public final TimeUnit            timeUnit;

        public Parameters(HashKey id, Member member, Supplier<Signature> signature, long frequency, TimeUnit timeUnit,
                int falsePositiveRate, SecureRandom entropy, MessagingMetrics metrics) {
            this.id = id;
            this.member = member;
            this.signature = signature;
            this.frequency = frequency;
            this.timeUnit = timeUnit;
            this.falsePositiveRate = falsePositiveRate;
            this.entropy = entropy;
            this.metrics = metrics;
        }
    }

    public class Service {
        public Messages gossip(MessageBff inbound) {
            return buffer.process(new BloomFilter(inbound.getDigests()), parameters.entropy.nextInt(),
                                  parameters.falsePositiveRate);
        }

        public void update(Push push) {
            List<Message> updates = push.getUpdatesList();

            process(updates);
        }
    }

    public static final Logger log = LoggerFactory.getLogger(Messenger.class);

    private final MessageBuffer                                       buffer;
    private final Map<Integer, MessageChannelHandler>                 channelHandlers = new ConcurrentHashMap<>();
    private final CommonCommunications<MessagingClientCommunications> comm;
    private final Context<Member>                                     context;
    private volatile int                                              lastRing        = -1;
    private final Parameters                                          parameters;
    private final AtomicBoolean                                       started         = new AtomicBoolean();

    public Messenger(Context<Member> context, MessageBuffer buffer, Communications communications,
            Parameters parameters) {
        this.parameters = parameters;
        this.context = context;
        this.buffer = buffer;
        this.comm = communications.create(parameters.member, getCreate(parameters.metrics),
                                          new MessagingServerCommunications(new Service(),
                                                  communications.getClientIdentityProvider(), parameters.metrics));
    }

    public void oneRound(ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }

        MessagingClientCommunications link = nextRing();

        if (link == null) {
            log.debug("No members to gossip with on ring: {}", lastRing);
            return;
        }
        log.trace("gossiping with {} on {}", link.getMember(), lastRing);
        try {
            Messages gossip = link.gossip(MessageBff.newBuilder()
                                                    .setContext(parameters.id.toID())
                                                    .setDigests(buffer.getBff(parameters.entropy.nextInt(),
                                                                              parameters.falsePositiveRate)
                                                                      .toBff())
                                                    .build());
            process(gossip.getUpdatesList());
            Builder builder = Push.newBuilder();
            buffer.updatesFor(new BloomFilter(gossip.getBff()), builder);
            link.update(builder.build());
        } finally {
            link.release();
        }
        if (started.get()) {
            scheduler.schedule(() -> oneRound(scheduler), parameters.frequency, parameters.timeUnit);
        }
    }

    public void publish(int channel, byte[] message, Signature signature) {
        buffer.put(System.currentTimeMillis(), message, parameters.member, signature, channel);
    }

    public void register(int channel, MessageChannelHandler listener) {
        channelHandlers.put(channel, listener);
    }

    public void start(ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        scheduler.schedule(() -> oneRound(scheduler), parameters.frequency, parameters.timeUnit);
    }

    public void stop() {
        started.set(false);
    }

    private MessagingClientCommunications linkFor(Integer ring) {
        Member successor = context.ring(ring).successor(parameters.member, m -> true);
        if (successor == null) {
            log.debug("No successor to node on ring: {} members: {}", ring, context.ring(ring).size());
            return null;
        }
        try {
            return comm.apply(successor, parameters.member);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", successor.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private MessagingClientCommunications nextRing() {
        MessagingClientCommunications link = null;
        int last = lastRing;
        int rings = context.getRings().length;
        int current = (last + 1) % rings;
        for (int i = 0; i < rings; i++) {
            link = linkFor(current);
            if (link != null) {
                break;
            }
            current = (current + 1) % rings;
        }
        lastRing = current;
        return link;
    }

    private void process(List<Message> updates) {
        Map<Integer, List<Msg>> newMessages = new HashMap<>();
        buffer.merge(updates, message -> validate(message)).stream().map(m -> {
            HashKey id = new HashKey(m.getSource());
            Member from = context.getMember(id);
            if (from == null) {
                log.trace("{} message from unknown member: {}", parameters.member, id);
                return null;
            } else {
                return new Msg(from, m.getChannel(), m.getContent().toByteArray());
            }
        }).filter(m -> m != null).forEach(msg -> {
            newMessages.computeIfAbsent(msg.channel, i -> new ArrayList<>()).add(msg);
        });
        channelHandlers.values()
                       .forEach(handler -> commonPool().execute(() -> newMessages.entrySet()
                                                                                 .forEach(e -> handler.message(e.getValue()))));
    }

    private boolean validate(Message message) {
        return buffer.validate(message, parameters.signature.get());
    }
}
