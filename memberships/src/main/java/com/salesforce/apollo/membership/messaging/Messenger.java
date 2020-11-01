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
import java.time.Duration;
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
import com.salesforce.apollo.protocols.Conversion;
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
        public static class Builder implements Cloneable {

            private int              bufferSize        = 1000;
            private SecureRandom     entropy;
            private double           falsePositiveRate = 0.25;
            private HashKey          id;
            private MessagingMetrics metrics;
            private int              tooOld            = 9;

            public Parameters build() {
                return new Parameters(id, falsePositiveRate, entropy, bufferSize, tooOld, metrics);
            }

            @Override
            public Builder clone() {
                try {
                    return (Builder) super.clone();
                } catch (CloneNotSupportedException e) {
                    throw new IllegalStateException(e);
                }
            }

            public int getBufferSize() {
                return bufferSize;
            }

            public SecureRandom getEntropy() {
                return entropy;
            }

            public double getFalsePositiveRate() {
                return falsePositiveRate;
            }

            public HashKey getId() {
                return id;
            }

            public MessagingMetrics getMetrics() {
                return metrics;
            }

            public int getTooOld() {
                return tooOld;
            }

            public Builder setBufferSize(int bufferSize) {
                this.bufferSize = bufferSize;
                return this;
            }

            public Builder setEntropy(SecureRandom entropy) {
                this.entropy = entropy;
                return this;
            }

            public Builder setFalsePositiveRate(double falsePositiveRate) {
                this.falsePositiveRate = falsePositiveRate;
                return this;
            }

            public Builder setId(HashKey id) {
                this.id = id;
                return this;
            }

            public Builder setMetrics(MessagingMetrics metrics) {
                this.metrics = metrics;
                return this;
            }

            public Builder setTooOld(int tooOld) {
                this.tooOld = tooOld;
                return this;
            }

        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public final int              bufferSize;
        public final SecureRandom     entropy;
        public final double           falsePositiveRate;
        public final HashKey          id;
        public final MessagingMetrics metrics;
        public final int              tooOld;

        public Parameters(HashKey id, double falsePositiveRate, SecureRandom entropy, int bufferSize, int tooOld,
                MessagingMetrics metrics) {
            this.id = id;
            this.falsePositiveRate = falsePositiveRate;
            this.entropy = entropy;
            this.metrics = metrics;
            this.bufferSize = bufferSize;
            this.tooOld = tooOld;
        }
    }

    public class Service {
        public Messages gossip(MessageBff inbound, HashKey from) {
            Member predecessor = context.ring(inbound.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.warn("Invalid inbound messages gossip from: {} on ring: {} - not predecessor", from,
                         inbound.getRing());
                return Messages.getDefaultInstance();
            }
            return buffer.process(new BloomFilter(inbound.getDigests()), parameters.entropy.nextInt(),
                                  parameters.falsePositiveRate);
        }

        public void update(Push push, HashKey from) {
            Member predecessor = context.ring(push.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.warn("Invalid inbound messages update from: {} on ring: {} - not predecessor", from,
                         push.getRing());
                return;
            }
            process(push.getUpdatesList());
        }
    }

    public static final Logger log = LoggerFactory.getLogger(Messenger.class);

    public final Member                                               member;
    public final Supplier<Signature>                                  signature;
    private final MessageBuffer                                       buffer;
    private final Map<Integer, MessageChannelHandler>                 channelHandlers = new ConcurrentHashMap<>();
    private final CommonCommunications<MessagingClientCommunications> comm;
    private final Context<Member>                                     context;
    private volatile int                                              lastRing        = -1;
    private final Parameters                                          parameters;
    private final AtomicBoolean                                       started         = new AtomicBoolean();

    public Messenger(Member member, Supplier<Signature> signature, Context<Member> context,
            Communications communications, Parameters parameters) {
        this.member = member;
        this.signature = signature;
        this.parameters = parameters;
        this.context = context;
        this.buffer = new MessageBuffer(parameters.bufferSize, parameters.tooOld);
        this.comm = communications.create(member, getCreate(parameters.metrics), new MessagingServerCommunications(
                new Service(), communications.getClientIdentityProvider(), parameters.metrics));
    }

    public Member getMember() {
        return member;
    }

    public void oneRound(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }
        commonPool().execute(() -> {
            MessagingClientCommunications link = nextRing();

            int ring = lastRing;
            if (link == null) {
                log.debug("No members to message gossip with on ring: {}", ring);
                return;
            }
            log.trace("message gossiping with {} on {}", link.getMember(), ring);
            try {
                com.salesfoce.apollo.proto.MessageBff.Builder builder = MessageBff.newBuilder();
                if (parameters.id != null) {
                    builder.setContext(parameters.id.toID());
                }
                Messages gossip = link.gossip(builder.setRing(ring)
                                                     .setDigests(buffer.getBff(parameters.entropy.nextInt(),
                                                                               parameters.falsePositiveRate)
                                                                       .toBff())
                                                     .build());
                process(gossip.getUpdatesList());
                Builder pushBuilder = Push.newBuilder();
                if (parameters.id != null) {
                    pushBuilder.setContext(parameters.id.toID());
                }
                pushBuilder.setRing(ring);
                buffer.updatesFor(new BloomFilter(gossip.getBff()), pushBuilder);
                link.update(pushBuilder.build());
            } finally {
                link.release();
            }
            if (started.get()) {
                scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(), TimeUnit.MILLISECONDS);
            }
        });
    }

    public void publish(int channel, byte[] message) {
        buffer.put(System.currentTimeMillis(), message, member, signature.get(), channel);
    }

    public void register(int channel, MessageChannelHandler listener) {
        channelHandlers.put(channel, listener);
    }

    public void start(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        started.set(false);
    }

    private MessagingClientCommunications linkFor(Integer ring) {
        Member successor = context.ring(ring).successor(member, m -> true);
        if (successor == null) {
            log.debug("No successor to node on ring: {} members: {}", ring, context.ring(ring).size());
            return null;
        }
        try {
            return comm.apply(successor, member);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", successor.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private MessagingClientCommunications nextRing() {
        MessagingClientCommunications link = null;
        int last = lastRing;
        int rings = context.getRingCount();
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
                log.trace("{} message from unknown member: {}", member, id);
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
        HashKey memberID = new HashKey(message.getSource());
        Member member = context.getMember(memberID);
        if (member == null) {
            log.debug("Non existent member: " + memberID);
            return false;
        }
        if (!MessageBuffer.validate(message, member.forVerification(Conversion.DEFAULT_SIGNATURE_ALGORITHM))) {
            log.trace("Did not validate message {} from {}", new HashKey(message.getId()), memberID);
            return false;
        }
        return true;
    }
}
