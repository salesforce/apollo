/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.rbc;

import static com.salesforce.apollo.membership.messaging.rbc.comms.RbcClient.getCreate;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Timer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.salesfoce.apollo.messaging.proto.AgedMessage;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesfoce.apollo.messaging.proto.Reconcile;
import com.salesfoce.apollo.messaging.proto.ReconcileContext;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.messaging.rbc.comms.RbcServer;
import com.salesforce.apollo.membership.messaging.rbc.comms.ReliableBroadcast;
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

        void message(Digest context, List<Msg> messages);
    }

    public record Msg(Digest source, ByteString content, Digest hash) {}

    public record Parameters(int bufferSize, int maxMessages, Context<Member> context, DigestAlgorithm digestAlgorithm,
                             SigningMember member, RbcMetrics metrics, double falsePositiveRate,
                             int deliveredCacheSize, Executor exec) {
        public static class Builder implements Cloneable {
            private int             bufferSize         = 500;
            private Context<Member> context;
            private int             deliveredCacheSize = 1_000;
            private DigestAlgorithm digestAlgorithm    = DigestAlgorithm.DEFAULT;
            private Executor        exec               = r -> r.run();
            private double          falsePositiveRate  = 0.125;
            private int             maxMessages        = 100;
            private SigningMember   member;
            private RbcMetrics   metrics;

            public Parameters build() {
                return new Parameters(bufferSize, maxMessages, context, digestAlgorithm, member, metrics,
                                      falsePositiveRate, deliveredCacheSize, exec);
            }

            @Override
            public Builder clone() {
                try {
                    return (Builder) super.clone();
                } catch (CloneNotSupportedException e) {
                    throw new IllegalStateException();
                }
            }

            public int getBufferSize() {
                return bufferSize;
            }

            public Context<Member> getContext() {
                return context;
            }

            public int getDeliveredCacheSize() {
                return deliveredCacheSize;
            }

            public DigestAlgorithm getDigestAlgorithm() {
                return digestAlgorithm;
            }

            public Executor getExec() {
                return exec;
            }

            public double getFalsePositiveRate() {
                return falsePositiveRate;
            }

            public int getMaxMessages() {
                return maxMessages;
            }

            public SigningMember getMember() {
                return member;
            }

            public RbcMetrics getMetrics() {
                return metrics;
            }

            public Parameters.Builder setBufferSize(int bufferSize) {
                this.bufferSize = bufferSize;
                return this;
            }

            public Parameters.Builder setContext(Context<Member> context) {
                this.context = context;
                return this;
            }

            public Builder setDeliveredCacheSize(int deliveredCacheSize) {
                this.deliveredCacheSize = deliveredCacheSize;
                return this;
            }

            public Parameters.Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
                this.digestAlgorithm = digestAlgorithm;
                return this;
            }

            public Builder setExec(Executor exec) {
                this.exec = exec;
                return this;
            }

            public Builder setFalsePositiveRate(double falsePositiveRate) {
                this.falsePositiveRate = falsePositiveRate;
                return this;
            }

            public Builder setMaxMessages(int maxMessages) {
                this.maxMessages = maxMessages;
                return this;
            }

            public Parameters.Builder setMember(SigningMember member) {
                this.member = member;
                return this;
            }

            public Parameters.Builder setMetrics(RbcMetrics metrics) {
                this.metrics = metrics;
                return this;
            }
        }

        public static Parameters.Builder newBuilder() {
            return new Builder();
        }

    }

    public class Service {

        public Reconcile gossip(MessageBff request, Digest from) {
            Member predecessor = params.context.ring(request.getRing()).predecessor(params.member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.info("Invalid inbound messages gossip on {}:{} from: {} on ring: {} - not predecessor: {}",
                         params.context.getId(), params.member, from, request.getRing(), predecessor);
                return Reconcile.getDefaultInstance();
            }
            return Reconcile.newBuilder()
                            .addAllUpdates(buffer.reconcile(BloomFilter.from(request.getDigests()), from))
                            .setDigests(buffer.forReconcilliation().toBff())
                            .build();
        }

        public void update(ReconcileContext reconcile, Digest from) {
            Member predecessor = params.context.ring(reconcile.getRing()).predecessor(params.member);
            if (predecessor == null || !from.equals(predecessor.getId())) {
                log.info("Invalid inbound messages reconcile on {}:{} from: {} on ring: {} - not predecessor: {}",
                         params.context.getId(), params.member, from, reconcile.getRing(), predecessor);
                return;
            }
            buffer.receive(reconcile.getUpdatesList());
        }
    }

    private class Buffer {
        private final Cache<Digest, Boolean> delivered;
        private final Semaphore              garbageCollecting = new Semaphore(1);
        private final int                    highWaterMark;
        private final int                    maxAge;
        private final AtomicInteger          nonce             = new AtomicInteger(0);
        private final AtomicInteger          round             = new AtomicInteger();
        private final Map<Digest, state>     state             = new ConcurrentHashMap<>();
        private final Semaphore              tickGate          = new Semaphore(1);

        public Buffer(int maxAge) {
            this.maxAge = maxAge;
            highWaterMark = (params.bufferSize - (int) (params.bufferSize + ((params.bufferSize) * 0.1)));
            delivered = CacheBuilder.newBuilder().maximumSize(params.deliveredCacheSize).build();
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
            deliver(messages.stream()
                            .limit(params.maxMessages)
                            .map(am -> new state(hashOf(am), AgedMessage.newBuilder(am), new Digest(am.getSource())))
                            .filter(s -> !s.from.equals(params.member.getId()))
                            .filter(s -> !dup(s))
                            .filter(s -> {
                                var from = params.context.getMember(s.from);
                                if (from == null) {
                                    return false;
                                }
                                var buff = ByteBuffer.allocate(4);
                                buff.putInt(s.msg.getNonce());
                                buff.flip();
                                boolean verify = from.verify(new JohnHancock(s.msg.getSignature()), buff,
                                                             s.msg.getContent().asReadOnlyByteBuffer());
                                return verify;
                            })
                            .map(s -> state.merge(s.hash, s, (a, b) -> a.msg.getAge() >= b.msg.getAge() ? a : b))
                            .map(s -> new Msg(s.from, s.msg.getContent(), s.hash))
                            .filter(m -> delivered(m.hash))
                            .toList());
            gc();
        }

        public Iterable<? extends AgedMessage> reconcile(BloomFilter<Digest> biff, Digest from) {
            PriorityQueue<AgedMessage.Builder> mailBox = new PriorityQueue<>(Comparator.comparingInt(s -> s.getAge()));
            state.values()
                 .stream()
                 .filter(s -> !biff.contains(s.hash))
                 .filter(s -> !s.from.equals(from))
                 .filter(s -> s.msg.getAge() < maxAge)
                 .forEach(s -> mailBox.add(s.msg));
            List<AgedMessage> reconciled = mailBox.stream().limit(params.maxMessages).map(b -> b.build()).toList();
            if (!reconciled.isEmpty()) {
                log.trace("reconciled: {} for: {} on: {}", reconciled.size(), from, params.member);
            }
            return reconciled;
        }

        public int round() {
            return round.get();
        }

        public AgedMessage send(ByteString msg, SigningMember member) {
            ByteBuffer buff = ByteBuffer.allocate(4);
            final int n = nonce.getAndIncrement();
            buff.putInt(n);
            buff.flip();
            final JohnHancock signature = member.sign(buff, msg.asReadOnlyByteBuffer());
            AgedMessage.Builder message = AgedMessage.newBuilder()
                                                     .setNonce(n)
                                                     .setSource(member.getId().toDigeste())
                                                     .setSignature(signature.toSig())
                                                     .setContent(msg);
            var hash = signature.toDigest(params.digestAlgorithm);
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

        private boolean delivered(Digest hash) {
            if (delivered.getIfPresent(hash) != null) {
                return false;
            }
            delivered.put(hash, true);
            return true;
        }

        private boolean dup(state s) {
            if (s.msg.getAge() > maxAge) {
                log.trace("Rejecting message too old: {} age: {} > {} on: {}", s.hash, s.msg.getAge(), maxAge,
                          params.member);
                return true;
            }
            var previous = state.get(s.hash);
            if (previous != null) {
                int nextAge = Math.max(previous.msg().getAge(), s.msg.getAge());
                if (nextAge > maxAge) {
                    state.remove(s.hash);
                } else if (previous.msg.getAge() != nextAge) {
                    previous.msg().setAge(nextAge);
                }
                log.trace("duplicate event: {} on: {}", s.hash, params.member);
                return true;
            }
            return delivered.getIfPresent(s.hash) != null;
        }

        private void gc() {
            if ((size() < highWaterMark) || !garbageCollecting.tryAcquire()) {
                return;
            }
            ForkJoinPool.commonPool().execute(Utils.wrapped(() -> {
                try {
                    int startSize = state.size();
                    if (startSize < highWaterMark) {
                        return;
                    }
                    log.trace("Compacting buffer: {} size: {} on: {}", params.context.getId(), startSize,
                              params.member);
                    purgeTheAged();
                    if (buffer.size() > params.bufferSize) {
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
            JohnHancock signature = JohnHancock.of(am.getSignature());
            return signature.toDigest(params.digestAlgorithm);
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
    private final Map<UUID, MessageHandler>                        channelHandlers = new ConcurrentHashMap<>();
    private final CommonCommunications<ReliableBroadcast, Service> comm;
    private final RingCommunications<ReliableBroadcast>            gossiper;
    private final Parameters                                       params;
    private final Map<UUID, Consumer<Integer>>                     roundListeners  = new ConcurrentHashMap<>();
    private final AtomicBoolean                                    started         = new AtomicBoolean();

    public ReliableBroadcaster(Parameters parameters, Router communications) {
        this.params = parameters;
        buffer = new Buffer(parameters.context.timeToLive() + 1);
        this.comm = communications.create(params.member, params.context.getId(), new Service(),
                                          r -> new RbcServer(communications.getClientIdentityProvider(),
                                                             parameters.metrics, r),
                                          getCreate(parameters.metrics),
                                          ReliableBroadcast.getLocalLoopback(params.member));
        gossiper = new RingCommunications<>(params.context, params.member, this.comm, params.exec);
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

    public void publish(ByteString message) {
        publish(message, false);
    }

    public void publish(ByteString message, boolean notifyLocal) {
        if (!started.get()) {
            return;
        }
        log.debug("publishing message on: {}", params.member);
        AgedMessage m = buffer.send(message, params.member);
        if (notifyLocal) {
            deliver(Collections.singletonList(new Msg(params.member.getId(), m.getContent(),
                                                      params.digestAlgorithm.digest(m.getContent()))));
        }
    }

    public void publish(Message message) {
        publish(message, false);
    }

    public void publish(Message message, boolean notifyLocal) {
        publish(message.toByteString(), notifyLocal);
    }

    public UUID register(Consumer<Integer> roundListener) {
        UUID reg = UUID.randomUUID();
        roundListeners.put(reg, roundListener);
        return reg;
    }

    public UUID registerHandler(MessageHandler listener) {
        UUID reg = UUID.randomUUID();
        channelHandlers.put(reg, listener);
        return reg;
    }

    public void removeHandler(UUID registration) {
        channelHandlers.remove(registration);
    }

    public void removeRoundListener(UUID registration) {
        roundListeners.remove(registration);
    }

    public void start(Duration duration, ScheduledExecutorService scheduler) {
        start(ForkJoinPool.commonPool(), duration, scheduler);
    }

    public void start(Executor exec, Duration duration, ScheduledExecutorService scheduler) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        Duration initialDelay = duration.plusMillis(Utils.bitStreamEntropy()
                                                         .nextInt((int) Math.max(1, duration.toMillis() * 2)));
        log.info("Starting Reliable Broadcaster[{}] for {}", params.context.getId(), params.member);
        comm.register(params.context.getId(), new Service());
        scheduler.schedule(() -> oneRound(exec, duration, scheduler), initialDelay.toMillis(), TimeUnit.MILLISECONDS);
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

    private void deliver(List<Msg> newMsgs) {
        if (newMsgs.isEmpty()) {
            return;
        }
        log.debug("Delivering: {} msgs for context: {} on: {} ", newMsgs.size(), params.context.getId(), params.member);
        channelHandlers.values().forEach(handler -> {
            try {
                handler.message(params.context.getId(), newMsgs);
            } catch (Throwable e) {
                log.warn("Error in message handler on: {}", params.member, e);
            }
        });
    }

    private ListenableFuture<Reconcile> gossipRound(ReliableBroadcast link, int ring) {
        if (!started.get()) {
            return null;
        }
        log.trace("rbc gossiping[{}] from {} with {} on {}", buffer.round(), params.member, link.getMember(), ring);
        try {
            return link.gossip(MessageBff.newBuilder()
                                         .setContext(params.context.getId().toDigeste())
                                         .setRing(ring)
                                         .setDigests(buffer.forReconcilliation().toBff())
                                         .build());
        } catch (Throwable e) {
            log.trace("rbc gossiping[{}] failed from {} with {} on {}", buffer.round(), params.member, link.getMember(),
                      ring, e);
            return null;
        }
    }

    private void handle(Executor exec, Optional<ListenableFuture<Reconcile>> futureSailor, ReliableBroadcast link,
                        int ring, Duration duration, ScheduledExecutorService scheduler, Timer.Context timer) {
        try {
            if (futureSailor.isEmpty()) {
                if (timer != null) {
                    timer.stop();
                }
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
            link.update(ReconcileContext.newBuilder()
                                        .setRing(ring)
                                        .setContext(params.context.getId().toDigeste())
                                        .addAllUpdates(buffer.reconcile(BloomFilter.from(gossip.getDigests()),
                                                                        link.getMember().getId()))
                                        .build());
        } finally {
            if (timer != null) {
                timer.stop();
            }
            if (started.get()) {
                try {
                    scheduler.schedule(() -> oneRound(exec, duration, scheduler), duration.toMillis(),
                                       TimeUnit.MILLISECONDS);
                } catch (RejectedExecutionException e) {
                    return;
                }
                buffer.tick();
                int gossipRound = buffer.round();
                roundListeners.values().forEach(l -> {
                    try {
                        l.accept(gossipRound);
                    } catch (Throwable e) {
                        log.error("error sending round() to listener: " + l, e);
                    }
                });
            }
        }
    }

    private void oneRound(Executor exec, Duration duration, ScheduledExecutorService scheduler) {
        if (!started.get()) {
            return;
        }

        exec.execute(() -> {
            var timer = params.metrics == null ? null : params.metrics.gossipRoundDuration().time();
            gossiper.execute((link, ring) -> gossipRound(link, ring),
                             (futureSailor, link, ring) -> handle(exec, futureSailor, link, ring, duration, scheduler,
                                                                  timer));
        });
    }
}
