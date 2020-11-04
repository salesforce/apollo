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
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
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
            private MessagingMetrics metrics;

            public Parameters build() {
                return new Parameters(falsePositiveRate, entropy, bufferSize, metrics);
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

            public MessagingMetrics getMetrics() {
                return metrics;
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

            public Builder setMetrics(MessagingMetrics metrics) {
                this.metrics = metrics;
                return this;
            }

        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public final int              bufferSize;
        public final SecureRandom     entropy;
        public final double           falsePositiveRate;
        public final MessagingMetrics metrics;

        public Parameters(double falsePositiveRate, SecureRandom entropy, int bufferSize, MessagingMetrics metrics) {
            this.falsePositiveRate = falsePositiveRate;
            this.entropy = entropy;
            this.metrics = metrics;
            this.bufferSize = bufferSize;
        }
    }

    public class Service {
        public Messages gossip(MessageBff inbound, HashKey from) {
            Member predecessor = context.ring(inbound.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.trace("Invalid inbound messages gossip on {}:{} from: {} on ring: {} - not predecessor: {}",
                          context.getId(), member, from, inbound.getRing(), predecessor);
                return Messages.getDefaultInstance();
            }
            return buffer.process(new BloomFilter(inbound.getDigests()), parameters.entropy.nextInt(),
                                  parameters.falsePositiveRate);
        }

        public void update(Push push, HashKey from) {
            Member predecessor = context.ring(push.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.trace("Invalid inbound messages update on: {}:{} from: {} on ring: {} - not predecessor: {}",
                          context.getId(), member, from, push.getRing(), predecessor);
                return;
            }
            process(push.getUpdatesList());
        }
    }

    public static final Logger log = LoggerFactory.getLogger(Messenger.class);

    public final Member                                                        member;
    public final Supplier<Signature>                                           signature;
    private final MessageBuffer                                                buffer;
    private final Map<Integer, MessageChannelHandler>                          channelHandlers = new ConcurrentHashMap<>();
    private final CommonCommunications<MessagingClientCommunications, Service> comm;
    private final Context<Member>                                              context;
    private volatile int                                                       lastRing        = -1;
    private final Parameters                                                   parameters;
    private final AtomicBoolean                                                started         = new AtomicBoolean();

    @SuppressWarnings("unchecked")
    public Messenger(Member member, Supplier<Signature> signature, Context<? extends Member> context,
            Router communications, Parameters parameters) {
        this.member = member;
        this.signature = signature;
        this.parameters = parameters;
        this.context = (Context<Member>) context;
        this.buffer = new MessageBuffer(parameters.bufferSize, context.timeToLive());
        this.comm = communications.create(member, context.getId(), new Service(),
                                          r -> new MessagingServerCommunications(
                                                  communications.getClientIdentityProvider(), parameters.metrics, r),
                                          getCreate(parameters.metrics));
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
            assert member.equals(context.ring(ring)
                                        .predecessor(link.getMember())) : "member is not the predecessor of the link!";
            log.debug("message gossiping from {} with {} on {}", member, link.getMember(), ring);
            try {
                Messages gossip = null;

                try {
                    gossip = link.gossip(MessageBff.newBuilder()
                                                   .setContext(context.getId().toID())
                                                   .setRing(ring)
                                                   .setDigests(buffer.getBff(parameters.entropy.nextInt(),
                                                                             parameters.falsePositiveRate)
                                                                     .toBff())
                                                   .build());
                } catch (Throwable e) {
                    log.debug("error gossipling with {}", link.getMember(), e);
                }
                if (gossip != null) {
                    process(gossip.getUpdatesList());
                    Builder pushBuilder = Push.newBuilder().setContext(context.getId().toID()).setRing(ring);
                    buffer.updatesFor(new BloomFilter(gossip.getBff()), pushBuilder);
                    try {
                        link.update(pushBuilder.build());
                    } catch (Throwable e) {
                        log.debug("error updating {}", link.getMember(), e);
                    }
                }
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
        comm.register(context.getId(), new Service());
        scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        comm.deregister(context.getId());
    }

    private MessagingClientCommunications linkFor(Integer ring) {
        Member successor = context.ring(ring).successor(member);
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
