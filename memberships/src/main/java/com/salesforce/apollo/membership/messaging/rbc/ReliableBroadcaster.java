/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.rbc;

import static com.salesforce.apollo.membership.messaging.comms.RbcClient.getCreate;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.messaging.proto.AgedMessage;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesfoce.apollo.messaging.proto.Reconcile;
import com.salesfoce.apollo.messaging.proto.Reconciliation;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.messaging.comms.RbcServer;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * Content agnostic reliable broadcast of messages.
 * 
 * @author hal.hildebrand
 *
 */
public class ReliableBroadcaster {
    @FunctionalInterface
    public interface MessageHandler {

        void message(Digest context, List<AgedMessage> messages);
    }

    public class Service {

        public Reconcile gossip(MessageBff request, Digest from) {
            Member predecessor = params.context.ring(request.getRing()).predecessor(params.member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.trace("Invalid inbound messages gossip on {}:{} from: {} on ring: {} - not predecessor: {}",
                          params.context.getId(), params.member, from, request.getRing(), predecessor);
                return Reconcile.getDefaultInstance();
            }
            return Reconcile.newBuilder().addAllUpdates(buffer.reconcile(BloomFilter.from(request.getDigests()), from))
                            .setBff(buffer.forReconcilliation().toBff()).build();
        }

        public void update(Reconciliation request, Digest from) {
            if (request.getRing() < 0 || request.getRing() >= params.context.getRingCount()) {
                log.trace("Invalid inbound messages update on {}:{} from: {} on invalid ring: {}",
                          params.context.getId(), params.member, from, request.getRing());
                return;
            }
            Member predecessor = params.context.ring(request.getRing()).predecessor(params.member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.trace("Invalid inbound messages update on: {}:{} from: {} on ring: {} - not predecessor: {}",
                          params.context.getId(), params.member, from, request.getRing(), predecessor);
                return;
            }
            buffer.receive(request.getUpdatesList());
        }
    }

    private class Buffer {
        private final Semaphore          garbageCollecting = new Semaphore(1);
        private final int                maxAge;
        private final AtomicInteger      round             = new AtomicInteger();
        private final Map<Digest, state> state             = new ConcurrentHashMap<>();
        private final Semaphore          tickGate          = new Semaphore(1);

        public Buffer(int maxAge) {
            this.maxAge = maxAge;
        }

        public void clear() {
            state.clear();
        }

        public BloomFilter<Digest> forReconcilliation() {
            var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(), params.bufferSize,
                                             params.falsePositiveRate);
            state.keySet().forEach(k -> biff.add(k));
            return biff;
        }

        public void receive(List<AgedMessage> messages) {
            if (messages.size() == 0) {
                return;
            }
            log.trace("receiving: {} msgs on: {}", messages.size(), params.member);
            deliver(messages.stream().limit(params.maxMessages)
                            .map(am -> new state(hashOf(am), AgedMessage.newBuilder(am), new Digest(am.getSource())))
                            .filter(s -> !s.from.equals(params.member.getId())).filter(s -> !dup(s)).filter(s -> {
                                var from = params.context.getMember(s.from);
                                if (from == null) {
                                    return false;
                                }
                                boolean verify = from.verify(new JohnHancock(s.msg.getSignature()), s.msg.getContent());
                                return verify;
                            }).map(s -> state.merge(s.hash, s, (a, b) -> a.msg.getAge() >= b.msg.getAge() ? a : b))
                            .map(s -> s.msg.build()).toList());
            gc();
        }

        public Iterable<? extends AgedMessage> reconcile(BloomFilter<Digest> biff, Digest from) {
            List<AgedMessage> reconciled = state.values().stream().filter(s -> !biff.contains(s.hash))
                                                .filter(s -> !s.from.equals(from)).filter(s -> s.msg.getAge() < maxAge)
                                                .limit(params.maxMessages).map(s -> s.msg.build()).toList();
            if (!reconciled.isEmpty()) {
                log.trace("reconciled: {} for: {} on: {}", reconciled.size(), from, params.member);
            }
            return reconciled;
        }

        public int round() {
            return round.get();
        }

        public AgedMessage send(byte[] content, SigningMember member) {
            AgedMessage.Builder message = AgedMessage.newBuilder().setSource(member.getId().toDigeste())
                                                     .setSignature(member.sign(content).toSig())
                                                     .setContent(ByteString.copyFrom(content));
            var hash = params.digestAlgorithm.digest(content);
            state s = new state(hash, message, member.getId());
            state.put(hash, s);
            log.trace("Send message:{} on: {}", hash, params.member);
            return s.msg.build();
        }

        public int size() {
            return state.size();
        }

        public void tick() {
            round.incrementAndGet();
            if (!tickGate.tryAcquire()) {
                log.trace("Unable to acquire tick gate for: {} tick already in progress on: {}", params.context.getId(),
                          params.member);
                return;
            }
            try {
                var trav = state.entrySet().iterator();
                while (trav.hasNext()) {
                    var next = trav.next().getValue();
                    int age = next.msg.getAge();
                    if (age >= maxAge) {
                        trav.remove();
                        log.trace("GC'ing: {} from: {} age: {} > {} on: {}", next.hash, next.from, age + 1, maxAge,
                                  params.member);
                    } else {
                        next.msg.setAge(age + 1);
                    }
                }
            } finally {
                tickGate.release();
            }
        }

        private boolean dup(state s) {
            if (s.msg.getAge() > maxAge) {
                log.trace("Rejecting message too old: {} age: {} > {} on: {}", s.hash, s.msg.getAge(), maxAge,
                          params.member);
                return false;
            }
            var previous = state.get(s.hash);
            if (previous != null) {
                int nextAge = Math.max(previous.msg().getAge(), s.msg.getAge());
                if (nextAge > maxAge) {
                    state.remove(s.hash);
                } else if (previous.msg.getAge() != nextAge) {
                    previous.msg().setAge(nextAge);
                }
//                log.debug("duplicate event: {} on: {}", hash, params.member);
                return true;
            }
            return false;
        }

        private void gc() {
            if (size() < params.bufferSize) {
                return;
            }
            if (!garbageCollecting.tryAcquire()) {
                return;
            }
            params.executor.execute(Utils.wrapped(() -> {
                try {
                    int startSize = state.size();
                    if (startSize < params.bufferSize) {
                        return;
                    }
                    log.trace("Compacting buffer: {} size: {} on: {}", params.context.getId(), startSize,
                              params.member);
                    purgeTheAged();
                    if (buffer.size() > (params.bufferSize + (((double) params.bufferSize) * 0.1))) {
                        log.warn("Buffer overflow: {} > {} after compact for: {} on: {} ", buffer.size(),
                                 params.bufferSize, params.context.getId(), params.member);
                    }
                    int freed = startSize - state.size();
                    if (freed > 0) {
                        log.debug("Buffer freed: {} after compact for: {} on: {} ", freed, params.context.getId(),
                                  params.member);
                    }
                } finally {
                    garbageCollecting.release();
                }
            }, log));

        }

        private Digest hashOf(AgedMessage am) {
            return params.digestAlgorithm.digest(am.getContent());
        }

        private void purgeTheAged() {
            log.debug("Purging the aged of: {} buffer size: {}   on: {}", params.context.getId(), size(),
                      params.member);
            Queue<state> candidates = new PriorityQueue<>(Collections.reverseOrder((a,
                                                                                    b) -> Integer.compare(a.msg.getAge(),
                                                                                                          b.msg.getAge())));
            candidates.addAll(state.values());
            var processing = candidates.iterator();
            while (processing.hasNext()) {
                var m = processing.next();
                if (m.msg.getAge() > maxAge) {
                    state.remove(m.hash);
                    log.trace("GC'ing: {} from: {} age: {} > {} on: {}", m.hash, m.from, m.msg.getAge() + 1, maxAge,
                              params.member);
                } else {
                    break;
                }
            }
        }

    }

    private record state(Digest hash, AgedMessage.Builder msg, Digest from) {};

    private static final Logger log = LoggerFactory.getLogger(ReliableBroadcaster.class);

    private final Buffer                                           buffer;
    private final List<MessageHandler>                             channelHandlers = new CopyOnWriteArrayList<>();
    private final CommonCommunications<ReliableBroadcast, Service> comm;
    private final RingCommunications<ReliableBroadcast>            gossiper;
    private final Parameters                                       params;
    private final List<Consumer<Integer>>                          roundListeners  = new CopyOnWriteArrayList<>();
    private final AtomicBoolean                                    started         = new AtomicBoolean();

    public ReliableBroadcaster(Parameters parameters, Router communications) {
        this.params = parameters;
        this.comm = communications.create(params.member, params.context.getId(), new Service(),
                                          r -> new RbcServer(communications.getClientIdentityProvider(),
                                                             parameters.metrics, r),
                                          getCreate(parameters.metrics, params.executor),
                                          ReliableBroadcast.getLocalLoopback(params.member));
        gossiper = new RingCommunications<>(params.context, params.member, this.comm, params.executor);
        buffer = new Buffer(parameters.context.timeToLive() + 1);
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
        return buffer.round();
    }

    public void publish(byte[] message) {
        publish(message, false);
    }

    public void publish(byte[] message, boolean notifyLocal) {
        if (!started.get()) {
            return;
        }
        AgedMessage m = buffer.send(message, params.member);
        if (notifyLocal) {
            deliver(Collections.singletonList(m));
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
        log.info("Starting Reliable Broadcaster[{}] for {}", params.context.getId(), params.member);
        comm.register(params.context.getId(), new Service());
        scheduler.schedule(() -> oneRound(duration, scheduler), initialDelay.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        log.info("Stopping Reliable Broadcaster[{}] for {}", params.context.getId(), params.member);
        buffer.clear();
        gossiper.reset();
        comm.deregister(params.context.getId());
    }

    private void deliver(List<AgedMessage> newMsgs) {
        if (newMsgs.isEmpty()) {
            return;
        }
        log.trace("Delivering: {} msgs for context: {} on: {} ", newMsgs.size(), params.context.getId(), params.member);
        channelHandlers.forEach(handler -> {
            try {
                handler.message(params.context.getId(), newMsgs);
            } catch (Throwable e) {
                log.error("Error in message handler on: {}", params.member, e);
            }
        });
    }

    private ListenableFuture<Reconcile> gossipRound(ReliableBroadcast link, int ring) {
        if (!started.get()) {
            return null;
        }
        log.trace("rbc gossiping[{}] from {} with {} on {}", buffer.round(), params.member, link.getMember(), ring);
        return link.gossip(MessageBff.newBuilder().setContext(params.context.getId().toDigeste()).setRing(ring)
                                     .setDigests(buffer.forReconcilliation().toBff()).build());
    }

    private void handle(Optional<ListenableFuture<Reconcile>> futureSailor, ReliableBroadcast link, int ring,
                        Duration duration, ScheduledExecutorService scheduler) {
        try {
            if (futureSailor.isEmpty()) {
                return;
            }
            Reconcile gossip;
            try {
                gossip = futureSailor.get().get();
            } catch (InterruptedException e) {
                log.debug("error gossiping with {}", link.getMember(), e);
                return;
            } catch (ExecutionException e) {
                log.debug("error gossiping with {}", link.getMember(), e.getCause());
                return;
            }
            buffer.receive(gossip.getUpdatesList());
            try {
                link.update(Reconciliation.newBuilder().setContext(params.context.getId().toDigeste()).setRing(ring)
                                          .addAllUpdates(buffer.reconcile(BloomFilter.from(gossip.getBff()),
                                                                          link.getMember().getId()))
                                          .build());
            } catch (Throwable e) {
                log.debug("error updating {}", link.getMember(), e);
            }
        } finally {
            if (started.get()) {
                scheduler.schedule(() -> oneRound(duration, scheduler), duration.toMillis(), TimeUnit.MILLISECONDS);
                buffer.tick();
                int gossipRound = buffer.round();
                roundListeners.forEach(l -> {
                    try {
                        l.accept(gossipRound);
                    } catch (Throwable e) {
                        log.error("error sending round() to listener: " + l, e);
                    }
                });
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
