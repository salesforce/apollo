/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.causal;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.messaging.proto.CausalMessage;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesfoce.apollo.utils.proto.StampedBloomeClock;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomClock;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;
import com.salesforce.apollo.utils.bloomFilters.ClockValue;

/**
 * @author hal.hildebrand
 *
 */
public class CausalBuffer {
    private record CausalityClock(Lock lock, BloomClock clock, java.time.Clock wallclock) {

        Instant instant() {
            return wallclock.instant();
        }

        Instant observe(Digest digest) {
            return locked(() -> {
                clock.add(digest);
                return wallclock.instant();
            }, lock);
        }

        StampedBloomeClock stamp(Digest digest) {
            return locked(() -> {
                clock.add(digest);
                Instant now = wallclock.instant();
                return StampedBloomeClock.newBuilder()
                                         .setStamp(Timestamp.newBuilder()
                                                            .setSeconds(now.getEpochSecond())
                                                            .setNanos(now.getNano()))
                                         .setClock(clock.toBloomeClock())
                                         .build();
            }, lock);
        }
    }

    private record Stream(Digest id, BloomClock clock, PriorityQueue<StampedMessage> queue,
            Comparator<ClockValue> comparator, Lock lock) {

        List<Received> observe(Instant observed, List<Digest> digests) {
            return locked(() -> {
                clock.addAll(digests);
                List<Received> ready = new ArrayList<>();
                StampedMessage next = queue.peek();
                while (next != null) {
                    int compared = comparator.compare(next.clock(), clock);
                    if (compared < 0) {
                        log.trace("event: {} is delivered", next.hash, observed);
                        queue.poll();
                        ready.add(new Received(next.hash(), next.message()));
                        next = queue.peek();
                    } else if (compared == 0) {
                        Timestamp ts = next.message().getClock().getStamp();
                        Instant sent = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
                        if (observed.isAfter(sent)) {
                            queue.poll();
                            ready.add(new Received(next.hash(), next.message()));
                            next = queue.peek();
                        } else {
                            log.trace("event: {} is before the current wall clock: {}", next.hash, observed);
                            return ready;
                        }
                    } else {
                        log.trace("event: {} is after the current clock", next.hash);
                        return ready;
                    }
                }
                return ready;
            }, lock);
        }

        void reconcile(BloomFilter<Digest> biff, int limit, List<CausalMessage> reconcilliation) {
            locked(() -> {
                queue.stream()
                     .filter(r -> !biff.contains(r.hash()))
                     .map(r -> r.message())
                     .limit(limit - reconcilliation.size())
                     .peek(m -> m.setAge(m.getAge() + 1))
                     .forEach(m -> reconcilliation.add(m.build()));
            }, lock);
        }

        boolean deliver(Digest hash, BloomClock stamp, CausalMessage message, Supplier<Boolean> verify) {
            if (!clock.validate(message.getClock())) {
                log.trace("Invalid clock for: {} from: {} ", hash, id);
                return false;
            }
            return locked(() -> {
                Iterator<StampedMessage> iterator = queue.iterator();
                while (iterator.hasNext()) {
                    StampedMessage next = iterator.next();
                    if (next.hash().equals(hash)) {
                        int age = next.message().getAge();
                        next.message().setAge(Math.max(age, message.getAge()));
                        log.trace("Duplicate: {} from: {} ", hash, id);
                        return false;
                    }
                }
                if (verify.get()) {
                    log.trace("Verifying: {} from: {} ", hash, id);
                    queue.add(new StampedMessage(hash, stamp, CausalMessage.newBuilder(message)));
                    return true;
                } else {
                    log.trace("Rejecting: {} could not verify from: {} ", hash, id);
                    return false;
                }
            }, lock);
        }

        void updateAge() {
            locked(() -> queue.forEach(s -> s.message().setAge(s.message().getAge() + 1)), lock);
        }

        void forReconcilliation(DigestBloomFilter biff) {
            locked(() -> queue.forEach(message -> biff.add(message.hash())), lock);
        }
    }

    private record StampedMessage(Digest hash, ClockValue clock, CausalMessage.Builder message)
            implements Comparable<StampedMessage> {

        @Override
        public int compareTo(StampedMessage o) {
            Timestamp a = message.getClock().getStamp();
            Timestamp b = o.message.getClock().getStamp();
            return Instant.ofEpochSecond(a.getSeconds(), a.getNanos())
                          .compareTo(Instant.ofEpochSecond(b.getSeconds(), b.getNanos()));
        }
    }

    private record Received(Digest hash, CausalMessage.Builder message) implements Comparable<Received> {

        @Override
        public int compareTo(Received o) {
            return Long.compare(message.getAge(), o.message.getAge());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Received other)) {
                return false;
            }
            return hash.equals(other.hash);
        }

        @Override
        public int hashCode() {
            return hash.hashCode();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(CausalBuffer.class);

    static <T> T locked(Callable<T> call, final Lock lock) {
        lock.lock();
        try {
            return call.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            lock.unlock();
        }
    }

    static void locked(Runnable call, final Lock lock) {
        lock.lock();
        try {
            call.run();
        } finally {
            lock.unlock();
        }
    }

    private final CausalityClock                             clock;
    private final ConcurrentMap<Digest, Received>            delivered = new ConcurrentHashMap<>();
    private final Consumer<Map<Digest, List<CausalMessage>>> delivery;
    private final Parameters                                 params;
    private final AtomicReference<Digest>                    previous  = new AtomicReference<>();
    private final AtomicInteger                              size      = new AtomicInteger();
    private final ConcurrentMap<Digest, Stream>              streams   = new ConcurrentHashMap<>();

    public CausalBuffer(Parameters parameters, BloomClock clock, Consumer<Map<Digest, List<CausalMessage>>> delivery) {
        this.params = parameters;
        this.clock = new CausalityClock(new ReentrantLock(), clock, params.wallclock);
        this.delivery = delivery;
        initPrevious();
    }

    private void initPrevious() {
        byte[] buff = new byte[params.digestAlgorithm.digestLength()];
        Utils.secureEntropy().nextBytes(buff);
        previous.set(new Digest(params.digestAlgorithm, buff));
    }

    public void clear() {
        // TODO Auto-generated method stub

    }

    public void deliver(List<CausalMessage> messages) {
        List<Digest> digests = messages.stream().map(message -> deliver(message)).filter(e -> e != null).toList();
        size.addAndGet(digests.size());
        observe(digests);

    }

    public DigestBloomFilter forReconcilliation(DigestBloomFilter biff) {
        streams.values().forEach(stream -> stream.forReconcilliation(biff));
        delivered.values().forEach(received -> biff.add(received.hash()));

        return biff;
    }

    public List<CausalMessage> reconcile(BloomFilter<Digest> biff) {
        List<CausalMessage> reconcilliation = new ArrayList<>();
        delivered.values()
                 .stream()
                 .filter(r -> !biff.contains(r.hash()))
                 .map(r -> r.message())
                 .limit(params.maxMessages - reconcilliation.size())
                 .peek(m -> m.setAge(m.getAge() + 1))
                 .forEach(m -> reconcilliation.add(m.build()));
        if (reconcilliation.size() >= params.maxMessages) {
            return reconcilliation;
        }
        streams.values().forEach(stream -> stream.reconcile(biff, params.maxMessages, reconcilliation));
        return reconcilliation;
    }

    public CausalMessage send(Any content, SigningMember member) {
        Digest prev = previous.get();
        Digest hash = params.digestAlgorithm.digest(prev.toDigeste().toByteString(), content.toByteString(),
                                                    member.getId().toDigeste().toByteString());

        StampedBloomeClock stamp = clock.stamp(hash);

        Sig sig = member.sign(hash.toDigeste().toByteString(), stamp.toByteString()).toSig();
        CausalMessage.Builder message = CausalMessage.newBuilder()
                                                     .setAge(0)
                                                     .setSource(member.getId().toDigeste())
                                                     .setClock(stamp)
                                                     .setContent(content)
                                                     .setHash(hash.toDigeste())
                                                     .setSignature(sig);
        delivered.put(hash, new Received(hash, message));
        size.incrementAndGet();
        previous.set(hash);
        observe(hash);
        return message.build();
    }

    public int size() {
        return size.get();
    }

    public void tick() {
        streams.values().forEach(stream -> stream.updateAge());
        delivered.values().forEach(r -> r.message().setAge(r.message().getAge() + 1));
    }

    private Digest deliver(CausalMessage message) {
        Digest hash = new Digest(message.getHash());
        Digest id = new Digest(message.getSource());
        Member from = params.context.getActiveMember(id);
        if (from == null) {
            log.trace("rejecting: {} as source is NULL on: {}", hash, params.member);
            return null;
        }

        Received previous = delivered.get(hash);
        if (previous != null) {
            previous.message().setAge(Math.max(previous.message().getAge(), message.getAge()));
            log.trace("duplicate: {} from: {} on: {}", hash, id, params.member);
            return null;
        }

        Timestamp ts = message.getClock().getStamp();
        Instant sent = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
        if (sent.plus(params.tooOld).isBefore(clock.instant())) {
            log.trace("rejecting: {} as to old: {} from: {} on: {}", hash, sent, id, params.member);
            return null;
        }

        BloomClock stamp = new BloomClock(message.getClock());

        if (stamp.isOrigin()) { // Reset of stream by the originating node
            streams.put(id, new Stream(id, new BloomClock(stamp, message.getStreamStart().toByteArray()),
                    new PriorityQueue<>(), params.comparator, new ReentrantLock()));
            log.info("Reset stream: {} event:{} on: {}", id, hash, params.member);
            return null;
        }

        return streams.computeIfAbsent(id, key -> {
            return new Stream(key, stamp, new PriorityQueue<>(), params.comparator, new ReentrantLock());
        }).deliver(hash, stamp, message, () -> verify(message, from)) ? hash : null;
    }

    private void observe(Digest hash) {
        observe(Collections.singletonList(hash), params.wallclock.instant());
    }

    private void observe(List<Digest> digests) {
        if (digests.isEmpty()) {
            return;
        }
        Instant observed = params.wallclock.instant();
        for (Digest hash : digests) {
            observed = clock.observe(hash);
        }
        log.trace("Observing: {} at: {} on: {}", digests, params.member);
        observe(digests, observed);
    }

    private void observe(List<Digest> digests, Instant observed) {
        Map<Digest, List<CausalMessage>> mail = new HashMap<>();
        streams.values().forEach(stream -> {
            var msgs = stream.observe(observed, digests)
                             .stream()
                             .peek(r -> delivered.put(r.hash(), r))
                             .map(r -> r.message().build())
                             .toList();
            if (!msgs.isEmpty()) {
                mail.put(stream.id(), msgs);
            }
        });

        if (!mail.isEmpty()) {
            delivery.accept(mail);
            if (log.isTraceEnabled()) {
                log.trace("Context: {} on: {} delivered: {}", params.context.getId(), params.member,
                          mail.values().stream().flatMap(msgs -> msgs.stream()).count());
            }
        }
    }

    private boolean verify(CausalMessage message, Member from) {
        return from.verify(new JohnHancock(message.getSignature()), message.getHash().toByteString(),
                           message.getClock().toByteString());
    }
}
