/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.causal;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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

    private record Stream(Digest id, BloomClock clock, TreeMap<StampedMessage, StampedMessage> queue,
            Comparator<ClockValue> comparator, Lock lock) {

        List<Received> observe(Instant observed, List<Digest> digests) {
            return locked(() -> {
                clock.addAll(digests);
                List<Received> ready = new ArrayList<>();
                var trav = queue.entrySet().iterator();
                while (trav.hasNext()) {
                    var entry = trav.next();
                    var next = entry.getValue();
                    int compared = comparator.compare(next.clock(), clock);
                    if (compared < 0) {
                        log.trace("event: {} is delivered", next.hash, observed);
                        ready.add(new Received(next.hash(), next.message()));
                        trav.remove();
                    } else if (compared == 0) {
                        Timestamp ts = next.message().getClock().getStamp();
                        Instant sent = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
                        if (observed.isAfter(sent)) {
                            ready.add(new Received(next.hash(), next.message()));
                            trav.remove();
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
                queue.values()
                     .stream()
                     .filter(r -> !biff.contains(r.hash()))
                     .map(r -> r.message())
                     .limit(limit - reconcilliation.size())
                     .peek(m -> m.setAge(m.getAge() + 1))
                     .forEach(m -> reconcilliation.add(m.build()));
            }, lock);
        }

        boolean deliver(Digest hash, BloomClock stamp, CausalMessage message, Supplier<Boolean> verify, int maxAge,
                        Duration tooOld, Instant now, AtomicInteger size) {
            if (!clock.validate(message.getClock())) {
                log.trace("Invalid clock for: {} from: {} ", hash, id);
                return false;
            }
            int age = message.getAge() + 1;
            return locked(() -> {
                Timestamp ts = message.getClock().getStamp();
                Instant sent = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
                StampedMessage candidate = new StampedMessage(hash, stamp, sent,
                        CausalMessage.newBuilder(message).setAge(age));
                var found = queue.get(candidate);
                if (found == null) {
                    if (verify.get()) {
                        log.trace("Verifying: {} from: {} ", hash, id);
                        queue.put(candidate, candidate);
                        return true;
                    } else {
                        log.trace("Rejecting: {} could not verify from: {} ", hash, id);
                        return false;
                    }
                } else {
                    log.trace("Duplicate: {} from: {} ", hash, id);
                    int nextAge = Math.max(age, found.message.getAge());
                    if (nextAge > maxAge || found.instant.plus(tooOld).isBefore(now)) {
                        log.trace("GC'ing: {} from: {} as too old: {} > {} : {}", hash, id, nextAge, maxAge,
                                  found.instant);
                        queue.remove(candidate);
                        size.decrementAndGet();
                    } else if (age != nextAge) {
                        log.trace("Updating age of: {} from: {} age: {} to: {}", hash, id, age, nextAge);
                        found.message().setAge(nextAge);
                    }
                    return false;
                }
            }, lock);
        }

        void updateAge() {
            locked(() -> queue.values().forEach(s -> s.message().setAge(s.message().getAge() + 1)), lock);
        }

        void forReconcilliation(DigestBloomFilter biff) {
            locked(() -> queue.values().forEach(message -> biff.add(message.hash())), lock);
        }

        public void purgeTheAged(int maxAge, Duration tooOld, Instant now, AtomicInteger size) {
            locked(() -> {
                log.trace("Purging the aged > {} from: {}", maxAge, id);
                var trav = queue.descendingMap().entrySet().iterator(); // oldest first
                while (trav.hasNext()) {
                    var next = trav.next().getValue();
                    if (next.message.getAge() > maxAge || next.instant.plus(tooOld).isBefore(now)) {
                        log.trace("GC'ing: {} from: {} as too old: {} > {} : {}", next.hash, id, next.message.getAge(),
                                  maxAge, next.instant);
                        trav.remove();
                        size.decrementAndGet();
                    }
                }
            }, lock);
        }
    }

    private record StampedMessage(Digest hash, ClockValue clock, Instant instant, CausalMessage.Builder message)
            implements Comparable<StampedMessage> {
        @Override
        public int compareTo(StampedMessage o) {
            if (hash.equals(o.hash)) {
                return 0;
            }
            return instant.compareTo(o.instant);
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
    private final int                                        maxAge;

    public CausalBuffer(Parameters parameters, BloomClock clock, Consumer<Map<Digest, List<CausalMessage>>> delivery) {
        this.params = parameters;
        this.clock = new CausalityClock(new ReentrantLock(), clock, params.wallclock);
        this.delivery = delivery;
        this.maxAge = params.context.timeToLive() + 1;
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
        gc();
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
                    new TreeMap<>(), params.comparator, new ReentrantLock()));
            log.info("Reset stream: {} event:{} on: {}", id, hash, params.member);
            return null;
        }

        return streams.computeIfAbsent(id, key -> {
            return new Stream(key, stamp, new TreeMap<>(), params.comparator, new ReentrantLock());
        })
                      .deliver(hash, stamp, message, () -> verify(message, from), maxAge, params.tooOld,
                               params.wallclock.instant(), size) ? hash : null;
    }

    private void gc() {
        int bufferSize = size.get();
        if (bufferSize < params.bufferSize) {
            return;
        }
        log.trace("Compacting buffer: {} size: {} on: {}", params.context.getId(), bufferSize, params.member);
        purgeTheAged();
        if (size.get() < params.bufferSize) {
            trimToSize();
        }
        int freed = bufferSize - size.get();
        if (freed > 0) {
            log.trace("Buffer freed: {} after compact for: {} on: {} ", freed, params.context.getId(), params.member);
        }
    }

    private void trimToSize() {
        int current = size.get();
        if (current < params.bufferSize) {
            return;
        }
        log.info("Trimming: {} current: {} target: {} on: {}", params.context.getId(), current, params.bufferSize,
                 params.member);

    }

    private void purgeTheAged() {
        log.trace("Purging the aged of: {} on: {}", params.context.getId(), params.member);
        Instant now = params.wallclock.instant();
        streams.values().forEach(stream -> stream.purgeTheAged(maxAge, params.tooOld, now, size));
        var trav = delivered.entrySet().iterator();
        while (trav.hasNext()) {
            var next = trav.next().getValue();
            if (next.message.getAge() > maxAge) {
                log.trace("GC'ing: {} as too old: {} > {} on: {}", next.hash, next.message.getAge(), maxAge,
                          params.member);
                trav.remove();
                size.decrementAndGet();
            }
        }
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
