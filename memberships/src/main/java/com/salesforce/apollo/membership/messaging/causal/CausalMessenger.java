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
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.utils.bloomFilters.BloomClock;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;
import com.salesforce.apollo.utils.bloomFilters.ClockValue;

/**
 * @author hal.hildebrand
 *
 */
public class CausalMessenger {
    record CausalityClock(Lock lock, BloomClock clock, java.time.Clock wallclock) {

        Instant instant() {
            return wallclock.instant();
        }

        ClockValue current() {
            return locked(() -> clock.current(), lock);
        }

        ClockValue merge(ClockValue b) {
            return locked(() -> clock.merge(b), lock);
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

    record Stream(Member member, BloomClock clock, PriorityQueue<StampedMessage> queue,
            Comparator<ClockValue> comparator, Lock lock) {

        List<Received> observe(Instant observed, Digest hash) {
            return locked(() -> {
                clock.add(hash);
                List<Received> ready = new ArrayList<>();
                StampedMessage next = queue.peek();
                while (next != null) {
                    int compared = comparator.compare(next.clock, clock);
                    if (compared < 0) {
                        queue.poll();
                        ready.add(new Received(next.hash, next.message));
                        next = queue.peek();
                    } else if (compared == 0) {
                        Timestamp ts = next.message.getClock().getStamp();
                        Instant sent = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
                        if (observed.isAfter(sent)) {
                            queue.poll();
                            ready.add(new Received(next.hash, next.message));
                            next = queue.peek();
                        }
                    } else {
                        return ready;
                    }
                }
                return ready;
            }, lock);
        }

        void reconcile(DigestBloomFilter biff, int limit, List<CausalMessage> reconcilliation) {
            locked(() -> {
                queue.stream()
                     .filter(r -> !biff.contains(r.hash))
                     .map(r -> r.message)
                     .limit(limit - reconcilliation.size())
                     .peek(m -> m.setAge(m.getAge() + 1))
                     .forEach(m -> reconcilliation.add(m.build()));
            }, lock);
        }

        void deliver(Digest hash, BloomClock stamp, CausalMessage message, Supplier<Boolean> verify,
                     Runnable onAccept) {
            locked(() -> {
                Iterator<StampedMessage> iterator = queue.iterator();
                while (iterator.hasNext()) {
                    StampedMessage next = iterator.next();
                    if (next.hash.equals(hash)) {
                        int age = next.message.getAge();
                        next.message.setAge(Math.max(age, message.getAge()));
                        return;
                    }
                }
                if (verify.get()) {
                    queue.add(new StampedMessage(hash, stamp, CausalMessage.newBuilder(message)));
                    onAccept.run();
                }
            }, lock);
        }
    }

    record StampedMessage(Digest hash, ClockValue clock, CausalMessage.Builder message)
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

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CausalMessenger.class);

    private static <T> T locked(Callable<T> call, final Lock lock) {
        lock.lock();
        try {
            return call.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            lock.unlock();
        }
    }

    private static void locked(Runnable call, final Lock lock) {
        lock.lock();
        try {
            call.run();
        } finally {
            lock.unlock();
        }
    }

    private final int                                        bufferSize;
    private final CausalityClock                             clock;
    private final Comparator<ClockValue>                     comparator;
    private final ConcurrentMap<Digest, Received>            delivered = new ConcurrentHashMap<>();
    private final DigestAlgorithm                            digestAlgorithm;
    private final AtomicInteger                              size      = new AtomicInteger();
    private final ConcurrentMap<Digest, Stream>              streams   = new ConcurrentHashMap<>();
    private final Duration                                   tooOld;
    private final Consumer<Map<Digest, List<CausalMessage>>> delivery;

    public CausalMessenger(Comparator<ClockValue> comparator, BloomClock clock, DigestAlgorithm digestAlgorithm,
            int bufferSize, java.time.Clock wallclock, Duration tooOld,
            Consumer<Map<Digest, List<CausalMessage>>> delivery) {
        this.comparator = comparator;
        this.clock = new CausalityClock(new ReentrantLock(), clock, wallclock);
        this.digestAlgorithm = digestAlgorithm;
        this.bufferSize = bufferSize;
        this.tooOld = tooOld;
        this.delivery = delivery;
    }

    public void deliver(CausalMessage message, Member from) {
        Digest hash = new Digest(message.getHash());
        Received previous = delivered.get(hash);
        if (previous != null) {
            previous.message.setAge(Math.max(previous.message.getAge(), message.getAge()));
            return;
        }
        Timestamp ts = message.getClock().getStamp();
        Instant sent = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
        if (sent.plus(tooOld).isBefore(clock.instant())) {
            return;
        }

        BloomClock stamp = new BloomClock(message.getClock());

        if (stamp.isOrigin()) { // Reset of stream by the originating node
            BloomClock streamClock = new BloomClock(stamp, message.getStreamStart().toByteArray());
            Stream stream = new Stream(from, streamClock, new PriorityQueue<>(), comparator, new ReentrantLock());
            streams.put(from.getId(), stream);
            return;
        }

        streams.computeIfAbsent(from.getId(), id -> {
            return new Stream(from, stamp, new PriorityQueue<>(), comparator, new ReentrantLock());
        }).deliver(hash, stamp, message, () -> verify(message, from), () -> observe(hash));
    }

    public DigestBloomFilter forReconcilliation(DigestBloomFilter biff) {
        streams.values().forEach(stream -> stream.queue.forEach(message -> biff.add(message.hash)));
        delivered.values().forEach(received -> biff.add(received.hash));

        return biff;
    }

    public List<CausalMessage> reconcile(DigestBloomFilter biff, int limit) {
        List<CausalMessage> reconcilliation = new ArrayList<>();
        delivered.values()
                 .stream()
                 .filter(r -> !biff.contains(r.hash))
                 .map(r -> r.message)
                 .limit(limit - reconcilliation.size())
                 .peek(m -> m.setAge(m.getAge() + 1))
                 .forEach(m -> reconcilliation.add(m.build()));
        if (reconcilliation.size() >= limit) {
            return reconcilliation;
        }
        streams.values().forEach(stream -> stream.reconcile(biff, limit, reconcilliation));
        return reconcilliation;
    }

    public CausalMessage send(Any content, Signer signer) {
        Digest hash = digestAlgorithm.digest(signer.sign(content.toByteString()).toByteString());

        StampedBloomeClock stamp = clock.stamp(hash);

        Sig sig = signer.sign(hash.toDigeste().toByteString(), stamp.toByteString()).toSig();
        CausalMessage.Builder message = CausalMessage.newBuilder()
                                                     .setAge(0)
                                                     .setClock(stamp)
                                                     .setContent(content)
                                                     .setHash(hash.toDigeste())
                                                     .setSignature(sig);
        delivered.put(hash, new Received(hash, message));
        observe(hash);
        return message.build();
    }

    public void tick() {
        streams.values().forEach(stream -> stream.queue.forEach(s -> s.message.setAge(s.message.getAge() + 1)));
        delivered.values().forEach(r -> r.message.setAge(r.message.getAge() + 1));
    }

    private void observe(Digest hash) {
        size.incrementAndGet();
        Instant observed = clock.observe(hash);
        Map<Digest, List<CausalMessage>> mail = new HashMap<>();
        streams.values().forEach(stream -> {
            List<Received> msgs = stream.observe(observed, hash);
            if (!msgs.isEmpty()) {
                mail.put(stream.member.getId(), msgs.stream().map(r -> r.message.build()).toList());
            }
            msgs.forEach(r -> delivered.put(r.hash, r));
        });
        delivery.accept(mail);
    }

    private boolean verify(CausalMessage message, Member from) {
        return from.verify(new JohnHancock(message.getSignature()), message.getHash().toByteString(),
                           message.getClock().toByteString());
    }
}
