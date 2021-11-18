/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.causal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.salesfoce.apollo.messaging.proto.CausalEnvelope;
import com.salesfoce.apollo.messaging.proto.CausalEnvelopeOrBuilder;
import com.salesfoce.apollo.utils.proto.CausalMessage;
import com.salesfoce.apollo.utils.proto.StampedClock;
import com.salesforce.apollo.causal.BloomClock;
import com.salesforce.apollo.causal.ClockValueComparator;
import com.salesforce.apollo.causal.IntCausalClock;
import com.salesforce.apollo.causal.IntStampedClockValue;
import com.salesforce.apollo.causal.StampedClockValue;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;
import com.salesforce.apollo.utils.bloomFilters.Hash;

/**
 * @author hal.hildebrand
 * @deprecated
 *
 */
public class CausalBuffer {
    public record StampedMessage(Digest from, Digest hash, IntStampedClockValue clock, CausalEnvelope.Builder message)
                                implements Comparable<StampedMessage> {

        @Override
        public StampedMessage clone() {
            return new StampedMessage(from, hash, clock, message.clone());
        }

        @Override
        public boolean equals(Object obj) {
            return hash.equals(obj);
        }

        @Override
        public int hashCode() {
            return hash.hashCode();
        }

        @Override
        public int compareTo(StampedMessage o) {
            return clock.instant().compareTo(o.clock().instant());
        }
    }

    record StreamedSM(Stream stream, StampedMessage sm) {}

    record Delivered(Digest event, Stream stream, List<StampedMessage> delivered) {}

    private static class AgeComparator implements Comparator<CausalEnvelope> {
        @Override
        public int compare(CausalEnvelope a, CausalEnvelope b) {
            return Integer.compare(a.getAge(), b.getAge());
        }
    }

    private record Stream(Digest from, IntCausalClock clock, ConcurrentSkipListMap<Digest, StampedMessage> queue) {

        @Override
        public int hashCode() {
            return from.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Stream)) {
                return false;
            }
            Stream other = (Stream) obj;
            return from.equals(other.from);
        }

        Delivered observe(ClockValueComparator comparator, StampedMessage sent, Map<Digest, StampedMessage> delivered) {
            var current = clock.observe(sent.hash);
            List<StampedMessage> ready = new ArrayList<>();
            var trav = queue.entrySet().iterator();
            while (trav.hasNext()) {
                var entry = trav.next();
                var next = entry.getValue();
                int compared = comparator.compare(next.clock, current);
                if (compared < 0 || (compared == 0 && sent.clock.instant() - current.instant() <= 1)) {
                    delivered.put(next.hash, next);
                    ready.add(next);
                    current = clock.merge(next.clock);
                    trav.remove();
                } else {
                    break;
                }
            }
//            log.trace("Observing: {} resulted in: {} deliveries, backlog: {} clock: {} head: {} from: {}", sent.hash,
//                      ready.size(), queue.size(), clock.instant(),
//                      queue.size() > 0 ? queue.firstKey().clock.instant() : "<->", from);
            return new Delivered(sent.hash, this, ready);
        }

        void reconcile(BloomFilter<Digest> biff, Queue<CausalEnvelope> mailBin, int maxAge) {
            queue.values().stream().filter(r -> r.message.getAge() < maxAge).filter(r -> !biff.contains(r.hash()))
                 .forEach(m -> mailBin.add(m.message().build()));
        }

        void updateAge(int maxAge) {
            var trav = queue.entrySet().iterator();
            while (trav.hasNext()) {
                var next = trav.next().getValue();
                int age = next.message.getAge();
                if (age >= maxAge) {
                    trav.remove();
                    log.trace("GC'ing: {} from: {} age: {} > {}", next.hash, next.from, age, maxAge);
                } else {
                    next.message().setAge(next.message().getAge() + 1);
                }
            }
            queue.values().forEach(s -> s.message().setAge(s.message().getAge() + 1));
        }

        int backlog() {
            return queue.size();
        }

        void forReconcilliation(DigestBloomFilter biff) {
            queue.values().forEach(message -> biff.add(message.hash()));
        }

        void purgeTheAged(int maxAge, Digest member) {
            if (queue.size() > 0) {
                log.trace("backlog: {} for: {} on: {}", queue.size(), from, member);
            }
            var trav = queue.entrySet().iterator();
            while (trav.hasNext()) {
                var m = trav.next().getValue();
                int age = m.message.getAge();
                if (age > maxAge) {
                    trav.remove();
                    log.trace("GC'ing: {} from: {} age: {} > {}", m.hash, m.from, age, maxAge);
                }
            }

        }

        public boolean trimAged() {
            return false;
        }

    }

    private static final AgeComparator AGE_COMPARATOR = new AgeComparator();
    private static final Logger        log            = LoggerFactory.getLogger(CausalBuffer.class);

    public static Digest hashOf(CausalEnvelope cm, DigestAlgorithm algorithm) {
        return JohnHancock.of(cm.getSignature()).toDigest(algorithm);
    }

    private final IntCausalClock                              clock;
    private final ClockValueComparator                        comparator;
    private final ConcurrentMap<Digest, StampedMessage>       delivered         = new ConcurrentHashMap<>();
    private final Consumer<Map<Digest, List<StampedMessage>>> delivery;
    private final Semaphore                                   garbageCollecting = new Semaphore(1);
    private final int                                         maxAge;
    private final Parameters                                  params;
    private final AtomicReference<Digest>                     previous          = new AtomicReference<>();
    private final AtomicInteger                               round             = new AtomicInteger();
    private final AtomicInteger                               sequenceNumber    = new AtomicInteger();
    private final ConcurrentMap<Digest, Stream>               streams           = new ConcurrentHashMap<>();
    private final Semaphore                                   tickGate          = new Semaphore(1);

    public CausalBuffer(Parameters parameters, Consumer<Map<Digest, List<StampedMessage>>> delivery) {
        this.params = parameters;
        this.clock = new IntCausalClock(new BloomClock(params.clockK, params.clockM), sequenceNumber,
                                        new ReentrantLock());
        this.delivery = delivery;
        this.maxAge = params.context.timeToLive();
        comparator = new ClockValueComparator(Hash.fpp(params.clockK, params.clockM, params.eventWindow));
        initPrevious();
    }

    public void clear() {
        round.set(0);
        delivered.clear();
        initPrevious();
        streams.clear();
        clock.reset();
    }

    public StampedClockValue<Integer> current() {
        return clock.current();
    }

    public DigestBloomFilter forReconcilliation() {
        var biff = new DigestBloomFilter(Utils.bitStreamEntropy().nextLong(), params.bufferSize,
                                         params.falsePositiveRate);
        streams.values().forEach(stream -> stream.forReconcilliation(biff));
        delivered.values().forEach(received -> biff.add(received.hash()));

        return biff;
    }

    public ClockValueComparator getComparator() {
        return comparator;
    }

    public void receive(List<CausalEnvelope> messages) {
        if (messages.size() == 0) {
            return;
        }
        log.trace("receiving: {} msgs on: {}", messages.size(), params.member);
        observe(messages.stream()
                        .map(cm -> new StampedMessage(new Digest(cm.getContent().getSource()),
                                                      hashOf(cm, params.digestAlgorithm),
                                                      IntStampedClockValue.from(cm.getContent().getClock()),
                                                      CausalEnvelope.newBuilder(cm)))
                        .filter(sm -> !isSelf(sm)).filter(f -> !dup(f.hash, f.message.getAge()))
                        .map(sm -> process(sm, streamOf(sm))).filter(e -> e != null)
                        .flatMap(ssm -> streams.values().stream().map(stream -> {
                            return stream.observe(comparator, ssm.sm, delivered);
                        })).filter(d -> !d.delivered.isEmpty())
                        .peek(d -> log.trace("event: {} produced: {} deliveries from: {} for: {} on: {}", d.event,
                                             d.delivered.size(), d.stream.from, params.context.getId(), params.member))
                        .collect(Collectors.groupingBy(d -> d.stream.from, () -> new HashMap<>(),
                                                       Collectors.flatMapping(d -> d.delivered.stream().sorted(),
                                                                              Collectors.toList()))));
        gc();
    }

    public List<CausalEnvelope> reconcile(BloomFilter<Digest> biff, Digest partner) {
        Queue<CausalEnvelope> mailBin = new PriorityQueue<>(AGE_COMPARATOR);

        delivered.values().stream().filter(r -> !biff.contains(r.hash())).filter(m -> !m.from.equals(partner))
                 .filter(m -> m.message.getAge() < maxAge).forEach(m -> mailBin.add(m.message.build()));
        streams.values().stream().filter(stream -> !stream.from.equals(partner))
               .forEach(stream -> stream.reconcile(biff, mailBin, maxAge));

        List<CausalEnvelope> reconciled = new ArrayList<>();

        while (!mailBin.isEmpty() && reconciled.size() < params.maxMessages) {
            CausalEnvelope cm = mailBin.poll();
            log.trace("reconciling: {} for: {} on: {}", hashOf(cm, params.digestAlgorithm), partner, params.member);
            reconciled.add(cm);
        }

        if (!reconciled.isEmpty()) {
            log.trace("reconciled: {} for: {} on: {}", reconciled.size(), partner, params.member);
        }
        return reconciled;
    }

    public int round() {
        return round.get();
    }

    public StampedMessage send(Any content, SigningMember member) {
        Digest prev = previous.get();

        StampedClock stamp = clock.stamp();
        CausalMessage message = CausalMessage.newBuilder().setSource(member.getId().toDigeste()).setClock(stamp)
                                             .setContent(content).addParents(prev.toDigeste()).build();
        var sig = member.sign(message.toByteString());
        var hash = sig.toDigest(params.digestAlgorithm);
        StampedMessage stamped = new StampedMessage(member.getId(), hash, IntStampedClockValue.from(stamp),
                                                    CausalEnvelope.newBuilder().setAge(0).setSignature(sig.toSig())
                                                                  .setContent(message));
        previous.compareAndSet(prev, hash);
        clock.observe(stamped.hash);
        clock.merge(stamped.clock);
        delivered.put(hash, stamped);
        log.trace("Send message:{} on: {}", hash, params.member);
        return stamped;
    }

    public int size() {
        return delivered.size() + streams.values().stream().mapToInt(s -> s.backlog()).sum();
    }

    public void tick() {
        round.incrementAndGet();
        if (!tickGate.tryAcquire()) {
            log.error("Unable to acquire tick gate for: {} tick already in progress on: {}", params.context.getId(),
                      params.member);
            return;
        }
        try {
//            log.trace("tick: {} buffer size: {} on: {}", round.get(), size(), params.member);
            streams.values().forEach(stream -> stream.updateAge(maxAge));
            var trav = delivered.entrySet().iterator();
            while (trav.hasNext()) {
                var next = trav.next().getValue();
                int age = next.message.getAge();
                if (age >= maxAge) {
                    trav.remove();
                    log.trace("GC'ing: {} from: {} age: {} > {} on: {}", next.hash, next.from, age + 1, maxAge,
                              params.member);
                } else {
                    next.message.setAge(age + 1);
                }
            }
        } finally {
            tickGate.release();
        }
    }

    private boolean dup(Digest hash, int age) {
        if (age > maxAge) {
            log.trace("Rejecting message too old: {} age: {} > {} on: {}", hash, age, maxAge, params.member);
            return false;
        }
        StampedMessage previous = delivered.get(hash);
        if (previous != null) {
            int nextAge = Math.max(previous.message().getAge(), age);
            if (nextAge > maxAge) {
                delivered.remove(hash);
            } else if (previous.message.getAge() != nextAge) {
                previous.message().setAge(nextAge);
            }
//            log.debug("duplicate event: {} on: {}", hash, params.member);
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
                int bufferSize = size();
                if (bufferSize < params.bufferSize) {
                    return;
                }
                log.trace("Compacting buffer: {} size: {} on: {}", params.context.getId(), bufferSize, params.member);
                purgeTheAged();
                int currentSize = size();
                if (currentSize > params.bufferSize) {
                    log.warn("Buffer overflow: {} > {} after compact for: {} on: {} ", currentSize, params.bufferSize,
                             params.context.getId(), params.member);
                }
                int freed = bufferSize - currentSize;
                if (freed > 0) {
                    log.debug("Buffer freed: {} after compact for: {} on: {} ", freed, params.context.getId(),
                              params.member);
                }
            } finally {
                garbageCollecting.release();
            }
        }, log));

    }

    private void gcDelivered(Iterator<StampedMessage> processing) {
        log.debug("GC'ing: {} as buffer too full: {} > {} on: {}", params.context.getId(), size(), params.bufferSize,
                  params.member);
        while (processing.hasNext() && params.bufferSize < size()) {
            var m = processing.next();
            int age = m.message.getAge();
            if (age > maxAge) {
                if (delivered.remove(m.hash) != null) {
                    log.trace("GC'ing: {} age: {} > {} on: {}", m.hash, age, maxAge, params.member);
                }
            }
        }
    }

    private void gcPending() {
        log.debug("GC'ing pending messages on: {} as buffer too full: {} > {} on: {}", params.context.getId(), size(),
                  params.bufferSize, params.member);
        boolean gcd;
        while (params.bufferSize > size()) {
            gcd = false;
            for (Stream stream : streams.values()) {
                if (stream.trimAged()) {
                    gcd = true;
                }
            }
            if (!gcd) {
                break;
            }
        }
    }

    private void initPrevious() {
        byte[] buff = new byte[params.digestAlgorithm.digestLength()];
        Utils.secureEntropy().nextBytes(buff);
        previous.set(new Digest(params.digestAlgorithm, buff));
    }

    private boolean isSelf(StampedMessage sm) {
        if (params.member.getId().equals(sm.from)) {
            log.trace("Rejecting self sent: {} on: {}", sm.hash, params.member);
            return true;
        }
        return false;
    }

    private void observe(Map<Digest, List<StampedMessage>> mail) {
        if (mail.size() > 0) {
            delivery.accept(mail);
            if (log.isTraceEnabled()) {
                int sum = mail.values().stream().mapToInt(msgs -> msgs.size()).sum();
                if (sum != 0) {
                    log.trace("Delivered: {} msgs for context: {} on: {} ", sum, params.context.getId(), params.member);
                }
            }
        }
    }

    private StreamedSM process(StampedMessage candidate, Stream stream) {
        log.trace("processing: {} from: {} on: {}", candidate.hash, stream.from(), params.member);
        StampedMessage found = stream.queue().get(candidate.hash);
        if (found == null) {
            if (verify(candidate.message, params.context.getActiveMember(candidate.from))) {
                log.trace("Verified: {} from: {} on: {}", candidate.hash, stream.from(), params.member);
                stream.queue().putIfAbsent(candidate.hash, candidate);
                return new StreamedSM(stream, candidate);
            } else {
                log.debug("Rejecting: {} could not verify from: {} on: {}", candidate.hash, stream.from(),
                          params.member);
                return null;
            }
        } else if (!candidate.message.getSignature().equals(found.message.getSignature())) {
            log.debug("Rejecting: {} as signature does match recorded from: {} on: {}", candidate.hash, stream.from(),
                      params.member);
            return null;
        }
        log.debug("Duplicate event: {} from: {} on: {}", candidate.hash, stream.from(), params.member);
        int age = candidate.message.getAge();
        int nextAge = Math.max(age, found.message.getAge());
        if (nextAge > maxAge) {
            stream.queue.remove(candidate.hash);
            log.trace("GC'ing: {} from: {} age: {} > {} on: {}", candidate.hash, candidate.from,
                      candidate.message.getAge() + 1, maxAge, params.member);
        } else if (age != nextAge) {
            found.message().setAge(nextAge);
        }
        return null;
    }

    private void purgeTheAged() {
        log.debug("Purging the aged of: {} buffer size: {} delivered: {} on: {}", params.context.getId(), size(),
                  delivered.size(), params.member);
        Queue<StampedMessage> candidates = new PriorityQueue<>(Collections.reverseOrder((a,
                                                                                         b) -> Integer.compare(a.message.getAge(),
                                                                                                               b.message.getAge())));
        candidates.addAll(delivered.values());
        var processing = candidates.iterator();
        while (processing.hasNext()) {
            var m = processing.next();
            if (m.message.getAge() > maxAge) {
                delivered.remove(m.hash);
                log.trace("GC'ing: {} from: {} age: {} > {} on: {}", m.hash, m.from, m.message.getAge() + 1, maxAge,
                          params.member);
            } else {
                break;
            }
        }
        streams.values().forEach(stream -> stream.purgeTheAged(maxAge, params.member.getId()));
        if (params.bufferSize < size()) {
            gcDelivered(processing);
        }
        if (params.bufferSize < size()) {
            gcPending();
        }
    }

    private Stream streamOf(StampedMessage candidate) {
        return streams.compute(candidate.from, (k, v) -> {
            if (v == null || candidate.message.getStreamReset()) {
                if (candidate.message.getStreamReset()) {
                    log.warn("Reset stream: {} event:{} on: {}", candidate.from, candidate.hash, params.member);
                }
                return new Stream(candidate.from,
                                  new IntCausalClock(new BloomClock(candidate.clock.toClock(), params.clockK,
                                                                    params.clockM),
                                                     new AtomicInteger(candidate.clock.stamp()), new ReentrantLock()),
                                  new ConcurrentSkipListMap<>());
            }
            return v;
        });

    }

    private boolean verify(CausalEnvelopeOrBuilder message, Member from) {
        return from.verify(new JohnHancock(message.getSignature()), message.getContent().toByteString());
    }
}
