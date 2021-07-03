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
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.messaging.proto.CausalMessage;
import com.salesfoce.apollo.messaging.proto.CausalMessageOrBuilder;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesfoce.apollo.utils.proto.StampedClock;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomClock;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;
import com.salesforce.apollo.utils.bloomFilters.ClockValue;
import com.salesforce.apollo.utils.bloomFilters.Hash.DigestHasher;

/**
 * @author hal.hildebrand
 *
 */
public class CausalBuffer {
    record Filtering(Digest hash, Digest id, Instant sent, CausalMessage message) {
    }

    private record CausalityClock(BloomClock clock, java.time.Clock wallclock, Lock lock) {

        Instant instant() {
            return wallclock.instant();
        }

        Instant observe(Digest digest) {
            return locked(() -> {
                clock.add(digest);
                return wallclock.instant();
            }, lock);
        }

        StampedClock stamp(Digest digest) {
            return locked(() -> {
                clock.add(digest);
                Instant now = wallclock.instant();
                return StampedClock.newBuilder()
                                   .setStamp(Timestamp.newBuilder()
                                                      .setSeconds(now.getEpochSecond())
                                                      .setNanos(now.getNano()))
                                   .setClock(clock.toClock())
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
                        if (observed.isAfter(next.instant)) {
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

        List<Digest> deliver(List<StampedMessage> messages, Function<StampedMessage, Boolean> verify, int maxAge,
                             Duration tooOld, Instant now, AtomicInteger size) {
            List<Digest> delivered = new ArrayList<>();
            for (StampedMessage candidate : messages) {
                StampedMessage found = locked(() -> queue.get(candidate), lock);
                if (found == null) {
                    if (verify.apply(candidate)) {
                        delivered.add(candidate.hash);
                        locked(() -> queue.put(candidate, candidate), lock);
                        log.trace("Verified: {} from: {} ", candidate.hash, id);
                        continue;
                    } else {
                        log.trace("Rejecting: {} could not verify from: {} ", candidate.hash, id);
                        continue;
                    }
                }
                log.trace("Duplicate: {} from: {} ", candidate.hash, id);
                int age = candidate.message.getAge();
                int nextAge = Math.max(age, found.message.getAge());
                if (nextAge > maxAge || found.instant.plus(tooOld).isBefore(now)) {
                    log.trace("GC'ing: {} from: {} as too old: {} > {} : {}", candidate.hash, id, nextAge, maxAge,
                              found.instant);
                    locked(() -> queue.remove(candidate), lock);
                    size.decrementAndGet();
                } else if (age != nextAge) {
                    log.trace("Updating age of: {} from: {} age: {} to: {}", candidate.hash, id, age, nextAge);
                    found.message().setAge(nextAge);
                }
            }
            return delivered;
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
    private final int                                        maxAge;
    private final Parameters                                 params;
    private final AtomicReference<Digest>                    previous  = new AtomicReference<>();
    private final AtomicInteger                              size      = new AtomicInteger();

    private final ConcurrentMap<Digest, Stream> streams = new ConcurrentHashMap<>();

    public CausalBuffer(Parameters parameters, Consumer<Map<Digest, List<CausalMessage>>> delivery) {
        this.params = parameters;
        this.clock = new CausalityClock(new BloomClock(seedFor(params.member.getId()), params.clockK, params.clockM),
                params.wallclock, new ReentrantLock());
        this.delivery = delivery;
        this.maxAge = params.context.timeToLive() + 1;
        initPrevious();
    }

    public void clear() {
        // TODO Auto-generated method stub

    }

    public void deliver(List<CausalMessage> messages) {
        Map<Digest, List<StampedMessage>> binned = new HashMap<>();
        messages.stream()
                .map(cm -> new Filtering(new Digest(cm.getHash()), new Digest(cm.getSource()), instantOf(cm), cm))
                .filter(f -> !dup(f))
                .filter(f -> !tooOld(f))
                .forEach(f -> {
                    binned.computeIfAbsent(f.id, k -> {
                        Member from = params.context.getActiveMember(f.id);
                        if (from == null) {
                            log.trace("rejecting: {} as source is not a member of: {} on: {}", f.hash, from,
                                      params.context.getId(), params.member);
                            return null;
                        }
                        return new ArrayList<>();
                    })
                          .add(new StampedMessage(f.hash, ClockValue.of(f.message.getClock().getClock()), f.sent,
                                  CausalMessage.newBuilder(f.message).setAge(f.message.getAge() + 1)));
                });
        List<List<Digest>> digests = deliver(binned);
        size.addAndGet(digests.stream().mapToInt(e -> e.size()).sum());
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

        StampedClock stamp = clock.stamp(hash);

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
        observe(Collections.singletonList(hash), params.wallclock.instant());
        return message.build();
    }

    public int size() {
        return size.get();
    }

    public void tick() {
        streams.values().forEach(stream -> stream.updateAge());
        delivered.values().forEach(r -> r.message().setAge(r.message().getAge() + 1));
    }

    private List<List<Digest>> deliver(Map<Digest, List<StampedMessage>> messages) {
        var accumulated = new ArrayList<List<Digest>>();
        for (Entry<Digest, List<StampedMessage>> entry : messages.entrySet()) {
            StampedMessage reset = entry.getValue()
                                        .stream()
                                        .filter(m -> m.message.getStreamReset())
                                        .findFirst()
                                        .orElse(null);
            if (reset != null) {// Reset of stream by the originating node
                var id = entry.getKey();
                streams.put(entry.getKey(),
                            new Stream(id,
                                    new BloomClock(seedFor(id), reset.clock.toClock(), params.clockK, params.clockM),
                                    new TreeMap<>(), params.comparator, new ReentrantLock()));
                log.info("Reset stream: {} event:{} on: {}", id, reset.hash, params.member);
            } else {
                var stream = streams.computeIfAbsent(entry.getKey(), id -> {
                    return new Stream(id, new BloomClock(seedFor(id), entry.getValue().get(0).clock.toClock(),
                            params.clockK, params.clockM), new TreeMap<>(), params.comparator, new ReentrantLock());
                });
                accumulated.add(stream.deliver(entry.getValue(),
                                               message -> verify(message.message,
                                                                 params.context.getActiveMember(entry.getKey())),
                                               maxAge, params.tooOld, params.wallclock.instant(), size));
            }
        }
        return accumulated;
    }

    private boolean dup(Filtering f) {
        Received previous = delivered.get(f.hash);
        if (previous != null) {
            previous.message().setAge(Math.max(previous.message().getAge(), f.message.getAge()));
            log.trace("duplicate: {} from: {} on: {}", f.hash, f.id, params.member);
            return true;
        }
        return false;
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

    private void initPrevious() {
        byte[] buff = new byte[params.digestAlgorithm.digestLength()];
        Utils.secureEntropy().nextBytes(buff);
        previous.set(new Digest(params.digestAlgorithm, buff));
    }

    private Instant instantOf(CausalMessage cm) {
        Timestamp ts = cm.getClock().getStamp();
        return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    private void observe(List<Digest> sent, Instant observed) {
        Map<Digest, List<CausalMessage>> mail = new HashMap<>();
        streams.values().forEach(stream -> {
            var msgs = stream.observe(observed, sent)
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

    private void observe(List<List<Digest>> sent) {
        var flattened = sent.stream().flatMap(e -> e.stream()).peek(hash -> clock.observe(hash)).toList();
        if (flattened.isEmpty()) {
            return;
        }
        Instant observed = params.wallclock.instant();
        log.trace("Observing: {} at: {} on: {}", flattened, observed, params.member);
        observe(flattened, observed);
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

    private long seedFor(Digest id) {
        DigestHasher hasher = new DigestHasher();
        hasher.establish(id, 0);
        hasher.process(params.context.getId());
        return hasher.getH1();
    }

    private boolean tooOld(Filtering f) {
        if (f.sent.plus(params.tooOld).isBefore(clock.instant())) {
            log.trace("rejecting: {} as to old: {} from: {} on: {}", f.hash, f.sent, f.id, params.member);
            return true;
        }
        return false;
    }

    private void trimToSize() {
        int current = size.get();
        if (current < params.bufferSize) {
            return;
        }
        log.info("Trimming: {} current: {} target: {} on: {}", params.context.getId(), current, params.bufferSize,
                 params.member);

    }

    private boolean verify(CausalMessageOrBuilder message, Member from) {
        return from.verify(new JohnHancock(message.getSignature()), message.getHash().toByteString(),
                           message.getClock().toByteString());
    }
}
