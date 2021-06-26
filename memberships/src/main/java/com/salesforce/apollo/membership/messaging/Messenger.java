/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import static com.salesforce.apollo.crypto.QualifiedBase64.digest;
import static com.salesforce.apollo.membership.messaging.comms.MessagingClientCommunications.getCreate;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
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
import com.salesfoce.apollo.messaging.proto.Message;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesfoce.apollo.messaging.proto.Messages;
import com.salesfoce.apollo.messaging.proto.Push;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.messaging.Messenger.MessageHandler.Msg;
import com.salesforce.apollo.membership.messaging.comms.MessagingServerCommunications;
import com.salesforce.apollo.utils.BloomFilter;
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
            if (push.getRing() < 0 || push.getRing() >= context.getRingCount()) {
                log.trace("Invalid inbound messages update on {}:{} from: {} on invalid ring: {}", context.getId(),
                          member, from, push.getRing());
                return;
            }
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

    private final MessageBuffer                            buffer;
    private final List<MessageHandler>                     channelHandlers = new CopyOnWriteArrayList<>();
    private final CommonCommunications<Messaging, Service> comm;
    private final Context<Member>                          context;
    private final RingCommunications<Messaging>            gossiper;
    private final SigningMember                            member;
    private final Parameters                               parameters;
    private final AtomicInteger                            round           = new AtomicInteger();
    private final List<Consumer<Integer>>                  roundListeners  = new CopyOnWriteArrayList<>();
    private final AtomicBoolean                            started         = new AtomicBoolean();

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
                                          getCreate(parameters.metrics, executor), Messaging.getLocalLoopback(member));
        gossiper = new RingCommunications<>(this.context, member, this.comm, executor);
    }

    public void clearBuffer() {
        log.warn("Clearing message buffer on: {}", member);
        buffer.clear();
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

    public void publish(com.google.protobuf.Message message) {
        publish(message, false);
    }

    public void publish(com.google.protobuf.Message message, boolean notifyLocal) {
        if (!started.get()) {
            return;
        }
        Message m = buffer.publish(Any.pack(message), member);

        if (notifyLocal) {
            List<Msg> newMessages = Arrays.asList(new Msg(member, m.getSequenceNumber(), m.getContent()));
            channelHandlers.forEach(handler -> {
                try {
                    handler.message(context.getId(), newMessages);
                } catch (Throwable e) {
                    log.error("Error in message handler on: {}", member, e);
                }
            });
        }
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
        gossiper.reset();
        round.set(0);
        comm.deregister(context.getId());
    }

    private ListenableFuture<Messages> gossipRound(Messaging link, int ring) {
        if (!started.get()) {
            return null;
        }
        int gossipRound = round.incrementAndGet();
        log.trace("message gossiping[{}] from {} with {} on {}", gossipRound, member, link.getMember(), ring);
        return link.gossip(MessageBff.newBuilder()
                                     .setContext(context.getId().toByteString())
                                     .setRing(ring)
                                     .setDigests(buffer.getBff(Utils.bitStreamEntropy().nextInt(),
                                                               parameters.falsePositiveRate)
                                                       .toBff())
                                     .build());
    }

    private void handle(Optional<ListenableFuture<Messages>> futureSailor, Messaging link, int ring, Duration duration,
                        ScheduledExecutorService scheduler) {
        try {
            if (futureSailor.isEmpty()) {
                return;
            }
            Messages gossip;
            try {
                gossip = futureSailor.get().get();
            } catch (InterruptedException e) {
                log.debug("error gossiping with {}", link.getMember(), e);
                return;
            } catch (ExecutionException e) {
                log.debug("error gossiping with {}", link.getMember(), e.getCause());
                return;
            }
            process(gossip.getUpdatesList());
            Push.Builder pushBuilder = Push.newBuilder().setContext(context.getId().toByteString()).setRing(ring);
            buffer.updatesFor(BloomFilter.from(gossip.getBff()), pushBuilder);
            try {
                link.update(pushBuilder.build());
            } catch (Throwable e) {
                log.debug("error updating {}", link.getMember(), e);
            }
        } finally {
            if (started.get()) {
                int gossipRound = round.get();
                roundListeners.forEach(l -> {
                    try {
                        l.accept(gossipRound);
                    } catch (Throwable e) {
                        log.error("error sending round() to listener: " + l, e);
                    }
                });
                scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(), TimeUnit.MILLISECONDS);
            }
        }
    }

    private void oneRound(Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }

        gossiper.execute((link, ring) -> gossipRound(link, ring),
                         (futureSailor, link, ring) -> handle(futureSailor, link, ring, duration, scheduler));
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
        Digest from = digest(message.getSource());
        Member member = context.getMember(from);
        if (member == null) {
            log.debug("Non existent member: " + from);
            return false;
        }
        JohnHancock sig = JohnHancock.from(message.getSignature());
        if (!MessageBuffer.validate(sig, member, message.getSequenceNumber(), message.getContent())) {
            log.error("Did not validate message {} from {}", message, from);
            return false;
        }
        Digest calculated = parameters.digestAlgorithm.digest(sig.toByteString());
        if (!calculated.equals(hash)) {
            log.error("Did not validate message {} from {}", message, from);
            return false;
        }
        return true;
    }
}
