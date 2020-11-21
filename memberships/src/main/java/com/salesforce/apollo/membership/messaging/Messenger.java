/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static com.salesforce.apollo.membership.messaging.comms.MessagingClientCommunications.getCreate;
import static java.util.concurrent.ForkJoinPool.commonPool;

import java.security.SecureRandom;
import java.security.Signature;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.protobuf.Any;
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
import com.salesforce.apollo.membership.messaging.comms.MessagingClientCommunications;
import com.salesforce.apollo.membership.messaging.comms.MessagingServerCommunications;
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
            public final Any    content;
            public final Member from;
            public final int    sequenceNumber;

            public Msg(Member from, int sequenceNumber, Any any) {
                this.from = from;
                this.sequenceNumber = sequenceNumber;
                this.content = any;
            }

            @Override
            public String toString() {
                return "Msg [from=" + from + ", sequence# =" + sequenceNumber + "]";
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

    private final MessageBuffer                                                buffer;
    private final List<MessageChannelHandler>                                  channelHandlers = new CopyOnWriteArrayList<>();
    private final CommonCommunications<MessagingClientCommunications, Service> comm;
    private final Context<Member>                                              context;
    private volatile int                                                       lastRing        = -1;
    private final Member                                                       member;
    private final Parameters                                                   parameters;
    private final Deque<Msg>                                                   pending         = new LinkedBlockingDeque<Msg>();
    private final AtomicInteger                                                round           = new AtomicInteger();
    private final List<Consumer<Integer>>                                      roundListeners  = new CopyOnWriteArrayList<>();
    private final Supplier<Signature>                                          signature;
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

    public Context<? extends Member> getContext() {
        return context;
    }

    public Member getMember() {
        return member;
    }

    public int getRound() {
        return round.get();
    }

    public void oneRound(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }
        MessagingClientCommunications link = nextRing();
        int ring = lastRing;
        if (link == null) {
            log.debug("No members to message gossip with on ring: {}", ring);
            return;
        }

        commonPool().execute(() -> {
            if (!started.get()) {
                return;
            }
            int gossipRound = round.incrementAndGet();
            log.trace("message gossiping[{}] from {} with {} on {}", gossipRound, member, link.getMember(), ring);
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
                roundListeners.forEach(l -> {
                    try {
                        l.accept(gossipRound);
                    } catch (Throwable e) {
                        log.error("error sending round() to listener: " + l, e);
                    }
                });
                List<Msg> msgs = new ArrayList<>();
                Msg msg = pending.poll();
                while (msg != null) {
                    msgs.add(msg);
                    msg = pending.poll();
                }
                if (!msgs.isEmpty()) {
                    channelHandlers.forEach(handler -> {
                        try {
                            handler.message(msgs);
                        } catch (Throwable e) {
                            log.error("Error in message handler on: {}", member, e);
                        }
                    });
                }
                scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(), TimeUnit.MILLISECONDS);
            }
        });
    }

    public void publish(com.google.protobuf.Message message) {
        if (!started.get()) {
            return;
        }
        buffer.publish(Any.pack(message), member, signature.get());
    }

    public void register(Consumer<Integer> roundListener) {
        roundListeners.add(roundListener);
    }

    public void registerHandler(MessageChannelHandler listener) {
        channelHandlers.add(listener);
    }

    public void start(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        log.info("Starting Messenger[{}] for {}", context.getId(), member);
        comm.register(context.getId(), new Service());
        scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping Messenger[{}] for {}", context.getId(), member);
        buffer.clear();
        lastRing = -1;
        round.set(0);
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
        if (updates.size() == 0) {
            return;
        }
        int newMessages = 0;
        buffer.merge(updates, (hash, message) -> validate(hash, message)).stream().map(m -> {
            HashKey id = new HashKey(m.getSource());
            if (member.getId().equals(id)) {
                log.trace("Ignoriing message from self");
                return null;
            }
            Member from = context.getMember(id);
            if (from == null) {
                log.trace("{} message from unknown member: {}", member, id);
                return null;
            } else {
                return new Msg(from, m.getSequenceNumber(), m.getContent());
            }
        }).filter(m -> m != null).forEach(msg -> {
            pending.add(msg);
        });
        if (newMessages == 0) {
            log.trace("No updates processed out of: {}", updates.size());
            return;
        }
        log.trace("processed {} updates", updates.size());
    }

    private boolean validate(HashKey hash, Message message) {
        HashKey memberID = new HashKey(message.getSource());
        Member member = context.getMember(memberID);
        if (member == null) {
            log.debug("Non existent member: " + memberID);
            return false;
        }
        if (!MessageBuffer.validate(hash, message, member.forVerification(Conversion.DEFAULT_SIGNATURE_ALGORITHM))) {
            log.trace("Did not validate message {} from {}", message, memberID);
            return false;
        }
        return true;
    }
}
