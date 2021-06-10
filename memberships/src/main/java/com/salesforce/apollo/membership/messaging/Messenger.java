/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.crypto.QualifiedBase64.signature;
import static com.salesforce.apollo.membership.messaging.comms.MessagingClientCommunications.getCreate;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.salesfoce.apollo.proto.Message;
import com.salesfoce.apollo.proto.MessageBff;
import com.salesfoce.apollo.proto.Messages;
import com.salesfoce.apollo.proto.Push;
import com.salesfoce.apollo.proto.Push.Builder;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.messaging.Messenger.MessageHandler.Msg;
import com.salesforce.apollo.membership.messaging.comms.MessagingClientCommunications;
import com.salesforce.apollo.membership.messaging.comms.MessagingServerCommunications;
import com.salesforce.apollo.protocols.BloomFilter;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class Messenger {

    @FunctionalInterface
    public interface MessageHandler {
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

        void message(Digest context, List<Msg> messages);
    }

    public static class Parameters {
        public static class Builder implements Cloneable {

            private int              bufferSize        = 1000;
            private DigestAlgorithm  digestAlgorithm   = DigestAlgorithm.DEFAULT;
            private double           falsePositiveRate = 0.25;
            private MessagingMetrics metrics;

            public Parameters build() {
                return new Parameters(falsePositiveRate, bufferSize, metrics, digestAlgorithm);
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

            public DigestAlgorithm getDigestAlgorithm() {
                return digestAlgorithm;
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

            public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
                this.digestAlgorithm = digestAlgorithm;
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
        public final DigestAlgorithm  digestAlgorithm;
        public final double           falsePositiveRate;
        public final MessagingMetrics metrics;

        public Parameters(double falsePositiveRate, int bufferSize, MessagingMetrics metrics,
                DigestAlgorithm digestAlgorithm) {
            this.falsePositiveRate = falsePositiveRate;
            this.metrics = metrics;
            this.bufferSize = bufferSize;
            this.digestAlgorithm = digestAlgorithm;
        }
    }

    public class Service {
        public Messages gossip(MessageBff inbound, Digest from) {
            Member predecessor = context.ring(inbound.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.trace("Invalid inbound messages gossip on {}:{} from: {} on ring: {} - not predecessor: {}",
                          context.getId(), member, from, inbound.getRing(), predecessor);
                return Messages.getDefaultInstance();
            }
            return buffer.process(BloomFilter.from(inbound.getDigests()), Utils.bitStreamEntropy().nextInt(),
                                  parameters.falsePositiveRate);
        }

        public void update(Push push, Digest from) {
            Member predecessor = context.ring(push.getRing()).predecessor(member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.trace("Invalid inbound messages update on: {}:{} from: {} on ring: {} - not predecessor: {}",
                          context.getId(), member, from, push.getRing(), predecessor);
                return;
            }
            process(push.getUpdatesList());
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Messenger.class);

    private final MessageBuffer                                                buffer;
    private final List<MessageHandler>                                         channelHandlers = new CopyOnWriteArrayList<>();
    private final CommonCommunications<MessagingClientCommunications, Service> comm;
    private final Context<Member>                                              context;
    private final Executor                                                     executor;
    private volatile int                                                       lastRing        = -1;
    private final SigningMember                                                member;
    private final Parameters                                                   parameters;
    private final AtomicInteger                                                round           = new AtomicInteger();
    private final List<Consumer<Integer>>                                      roundListeners  = new CopyOnWriteArrayList<>();
    private final AtomicBoolean                                                started         = new AtomicBoolean();

    @SuppressWarnings("unchecked")
    public Messenger(SigningMember member, Context<? extends Member> context, Router communications,
            Parameters parameters, Executor executor) {
        this.member = member;
        this.parameters = parameters;
        this.context = (Context<Member>) context;
        this.buffer = new MessageBuffer(parameters.digestAlgorithm, parameters.bufferSize, context.timeToLive());
        this.comm = communications.create(member, context.getId(), new Service(),
                                          r -> new MessagingServerCommunications(
                                                  communications.getClientIdentityProvider(), parameters.metrics, r),
                                          getCreate(parameters.metrics));
        this.executor = executor;
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

        executor.execute(() -> {
            if (!started.get()) {
                return;
            }
            int gossipRound = round.incrementAndGet();
            log.trace("message gossiping[{}] from {} with {} on {}", gossipRound, member, link.getMember(), ring);
            ListenableFuture<Messages> futureSailor;

            try {
                futureSailor = link.gossip(MessageBff.newBuilder()
                                                     .setContext(qb64(context.getId()))
                                                     .setRing(ring)
                                                     .setDigests(buffer.getBff(Utils.bitStreamEntropy().nextInt(),
                                                                               parameters.falsePositiveRate)
                                                                       .toBff())
                                                     .build());
            } catch (Throwable e) {
                log.debug("error gossiping with {}", link.getMember(), e);
                return;
            }
            futureSailor.addListener(() -> {
                try {
                    Messages gossip;
                    try {
                        gossip = futureSailor.get();
                    } catch (InterruptedException e) {
                        log.debug("error gossiping with {}", link.getMember(), e);
                        return;
                    } catch (ExecutionException e) {
                        log.debug("error gossiping with {}", link.getMember(), e.getCause());
                        return;
                    }
                    process(gossip.getUpdatesList());
                    Builder pushBuilder = Push.newBuilder().setContext(qb64(context.getId())).setRing(ring);
                    buffer.updatesFor(BloomFilter.from(gossip.getBff()), pushBuilder);
                    try {
                        link.update(pushBuilder.build());
                    } catch (Throwable e) {
                        log.debug("error updating {}", link.getMember(), e);
                    }
                } finally {
                    link.release();
                    if (started.get()) {
                        roundListeners.forEach(l -> {
                            try {
                                l.accept(gossipRound);
                            } catch (Throwable e) {
                                log.error("error sending round() to listener: " + l, e);
                            }
                        });
                        scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(),
                                           TimeUnit.MILLISECONDS);
                    }
                }
            }, executor);
        });
    }

    public void publish(com.google.protobuf.Message message) {
        if (!started.get()) {
            return;
        }
        buffer.publish(Any.pack(message), member);
    }

    public void register(Consumer<Integer> roundListener) {
        roundListeners.add(roundListener);
    }

    public void registerHandler(MessageHandler listener) {
        channelHandlers.add(listener);
    }

    public void start(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        Duration initialDelay = duration.plusMillis(Utils.bitStreamEntropy().nextInt((int) (duration.toMillis() / 2)));
        log.info("Starting Messenger[{}] for {}", context.getId(), member);
        comm.register(context.getId(), new Service());
        scheduler.schedule(() -> oneRound(duration, scheduler), initialDelay.toMillis(), TimeUnit.MILLISECONDS);
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
        List<Msg> newMessages = buffer.merge(updates, (hash, message) -> validate(hash, message)).stream().map(m -> {
            Digest id = digest(m.getSource());
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
        }).filter(m -> m != null).collect(Collectors.toList());

        if (newMessages.isEmpty()) {
            log.trace("No updates processed out of: {}", updates.size());
            return;
        }
        log.trace("processed {} updates", updates.size());
        if (!newMessages.isEmpty()) {
            channelHandlers.forEach(handler -> {
                try {
                    handler.message(context.getId(), newMessages);
                } catch (Throwable e) {
                    log.error("Error in message handler on: {}", member, e);
                }
            });
        }
    }

    private boolean validate(Digest hash, Message message) {
        Digest memberID = digest(message.getSource());
        Member member = context.getMember(memberID);
        if (member == null) {
            log.debug("Non existent member: " + memberID);
            return false;
        }
        if (!MessageBuffer.validate(hash, message, member, signature(message.getSignature()))) {
            log.trace("Did not validate message {} from {}", message, memberID);
            return false;
        }
        return true;
    }
}
