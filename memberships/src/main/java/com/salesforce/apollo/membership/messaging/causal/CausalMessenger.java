/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.causal;

import static com.salesforce.apollo.membership.messaging.comms.CausalMessagingClient.getCreate;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.salesfoce.apollo.messaging.proto.CausalMessage;
import com.salesfoce.apollo.messaging.proto.CausalMessages;
import com.salesfoce.apollo.messaging.proto.CausalPush;
import com.salesfoce.apollo.messaging.proto.Message;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.messaging.causal.CausalMessenger.MessageHandler.Msg;
import com.salesforce.apollo.membership.messaging.comms.CausalMessagingServer;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomClock;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * @author hal.hildebrand
 *
 */
public class CausalMessenger {
    @FunctionalInterface
    public interface MessageHandler {
        class Msg {
            public final Any    content;
            public final Digest from;

            public Msg(Digest from, Any any) {
                this.from = from;
                this.content = any;
            }

            @Override
            public String toString() {
                return "Msg [from=" + from + "]";
            }
        }

        void message(Digest context, List<Msg> messages);
    }

    public class Service {

        public CausalMessages gossip(MessageBff request, Digest from) {
            // TODO Auto-generated method stub
            return null;
        }

        public void update(CausalPush request, Digest from) {
            // TODO Auto-generated method stub

        }

    }

    private static final Logger log = LoggerFactory.getLogger(CausalMessenger.class);

    private final CausalBuffer                                   buffer;
    private final List<MessageHandler>                           channelHandlers = new CopyOnWriteArrayList<>();
    private final CommonCommunications<CausalMessaging, Service> comm;
    private final RingCommunications<CausalMessaging>            gossiper;
    private final Parameters                                     params;
    private final AtomicInteger                                  round           = new AtomicInteger();
    private final List<Consumer<Integer>>                        roundListeners  = new CopyOnWriteArrayList<>();
    private final AtomicBoolean                                  started         = new AtomicBoolean();

    public CausalMessenger(Parameters parameters, BloomClock clock, Router communications) {
        this.params = parameters;
        this.comm = communications.create(params.member, params.context.getId(), new Service(),
                                          r -> new CausalMessagingServer(communications.getClientIdentityProvider(),
                                                  parameters.metrics, r),
                                          getCreate(parameters.metrics, params.executor),
                                          CausalMessaging.getLocalLoopback(params.member));
        gossiper = new RingCommunications<>(params.context, params.member, this.comm, params.executor);
        buffer = new CausalBuffer(parameters, clock, mail -> deliver(mail));
    }

    public void clearBuffer() {
        log.warn("Clearing message buffer on: {}", params.member);
        buffer.clear();
    }

    public Context<? extends Member> getContext() {
        return params.context;
    }

    public Member getMember() {
        return params.member;
    }

    public int getRound() {
        return round.get();
    }

    public void publish(Message message) {
        publish(message, false);
    }

    public void publish(Message message, boolean notifyLocal) {
        if (!started.get()) {
            return;
        }
        CausalMessage m = buffer.send(Any.pack(message), params.member);
        if (notifyLocal) {
            deliver(Map.of(params.member.getId(), Collections.singletonList(m)));
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
        log.info("Starting Causal Messenger[{}] for {}", params.context.getId(), params.member);
        comm.register(params.context.getId(), new Service());
        scheduler.schedule(() -> oneRound(duration, scheduler), initialDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping Causal Messenger[{}] for {}", params.context.getId(), params.member);
        buffer.clear();
        gossiper.reset();
        round.set(0);
        comm.deregister(params.context.getId());
    }

    private void deliver(Map<Digest, List<CausalMessage>> mail) {
        if (mail.isEmpty()) {
            return;
        }
        List<Msg> newMsgs = mail.entrySet()
                                .stream()
                                .flatMap(e -> e.getValue().stream().map(m -> new Msg(e.getKey(), m.getContent())))
                                .toList();
        channelHandlers.forEach(handler -> {
            try {
                handler.message(params.context.getId(), newMsgs);
            } catch (Throwable e) {
                log.error("Error in message handler on: {}", params.member, e);
            }
        });
    }

    private ListenableFuture<CausalMessages> gossipRound(CausalMessaging link, int ring) {
        if (!started.get()) {
            return null;
        }
        int gossipRound = round.incrementAndGet();
        log.trace("causal gossiping[{}] from {} with {} on {}", gossipRound, params.member, link.getMember(), ring);
        DigestBloomFilter biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(), params.bufferSize,
                params.falsePositiveRate);
        return link.gossip(MessageBff.newBuilder()
                                     .setContext(params.context.getId().toDigeste())
                                     .setRing(ring)
                                     .setDigests(buffer.forReconcilliation(biff).toBff())
                                     .build());
    }

    private void handle(Optional<ListenableFuture<CausalMessages>> futureSailor, CausalMessaging link, int ring,
                        Duration duration, ScheduledExecutorService scheduler) {
        try {
            if (futureSailor.isEmpty()) {
                return;
            }
            CausalMessages gossip;
            try {
                gossip = futureSailor.get().get();
            } catch (InterruptedException e) {
                log.debug("error gossiping with {}", link.getMember(), e);
                return;
            } catch (ExecutionException e) {
                log.debug("error gossiping with {}", link.getMember(), e.getCause());
                return;
            }
            buffer.deliver(gossip.getUpdatesList());
            try {
                link.update(CausalPush.newBuilder()
                                      .setContext(params.context.getId().toDigeste())
                                      .setRing(ring)
                                      .addAllUpdates(buffer.reconcile(BloomFilter.from(gossip.getBff())))
                                      .build());
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
}
