/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.causal;

import static com.salesforce.apollo.utils.Utils.locked;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.salesfoce.apollo.messaging.proto.CausalMessage;
import com.salesfoce.apollo.messaging.proto.CausalMessageOrBuilder;
import com.salesfoce.apollo.utils.proto.IntStampedClock;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesforce.apollo.causal.BloomClock;
import com.salesforce.apollo.causal.IntCausalClock;
import com.salesforce.apollo.causal.IntStampedClockValue;
import com.salesforce.apollo.causal.StampedClockValueComparator;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;
import com.salesforce.apollo.utils.bloomFilters.Hash;

/**
 * @author hal.hildebrand
 *
 */
public class CausalBuffer {
    public record StampedMessage(Digest from, Digest hash, IntStampedClockValue clock, CausalMessage.Builder message)
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

    private static class AgeComparator implements Comparator<CausalMessage> {
        @Override
        public int compare(CausalMessage a, CausalMessage b) {
            return Integer.compare(a.getAge(), b.getAge());
        }
    }

    private record Stream(Digest from, IntCausalClock clock, TreeMap<StampedMessage, StampedMessage> queue, Lock lock) {

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

        Delivered observe(StampedClockValueComparator<Integer, IntStampedClock> comparator, StampedMessage sent) {
            return locked(() -> {
                var current = clock.observe(sent.hash);
                List<StampedMessage> ready = new ArrayList<>();
                var trav = queue.entrySet().iterator();
                while (trav.hasNext()) {
                    var entry = trav.next();
                    var next = entry.getValue();
                    int compared = comparator.compare(next.clock, current);
                    if (compared >= 0) {
                        ready.add(next);
                        current = clock.merge(next.clock);
                        trav.remove();
                    } else {
                        break;
                    }
                }
                return new Delivered(this, ready);
            }, lock);
        }

        void reconcile(BloomFilter<Digest> biff, Queue<CausalMessage> mailBin, int maxAge) {
            locked(() -> {
                queue.values().stream().filter(r -> r.message.getAge() < maxAge).filter(r -> !biff.contains(r.hash()))
                     .forEach(m -> mailBin.add(m.message().build()));
            }, lock);
        }

        int updateAge(int maxAge) {
            return locked(() -> {
                int purged = 0;
                var trav = queue.entrySet().iterator();
                while (trav.hasNext()) {
                    var next = trav.next().getValue();
                    if (next.message.getAge() >= maxAge) {
                        trav.remove();
                        log.trace("GC'ing: {} from: {} as buffer too full: {} > {} on: {}", next.hash, next.from,
                                  maxAge);
                        purged++;
                    } else {
                        next.message().setAge(next.message().getAge() + 1);
                    }
                }
                queue.values().forEach(s -> s.message().setAge(s.message().getAge() + 1));
                return purged;
            }, lock);
        }

        void forReconcilliation(DigestBloomFilter biff) {
            locked(() -> queue.values().forEach(message -> biff.add(message.hash())), lock);
        }

        int purgeTheAged(int maxAge, Digest member) {
            return locked(() -> {
                if (queue.size() > 0) {
                    log.trace("backlog: {} for: {} on: {}", queue.size(), from, member);
                }
                int purged = 0;
                var trav = queue.entrySet().iterator();
                while (trav.hasNext()) {
                    var m = trav.next().getValue();
                    if (m.message.getAge() > maxAge) {
                        purged++;
                        trav.remove();
                        log.trace("GC'ing: {} from: {} as buffer too full: {} > {} on: {}", m.hash, m.from, maxAge);
                    }
                }
                return purged;
            }, lock);

        }

        public int trimAged() {
            return 0;
        }

    }

    private static final AgeComparator AGE_COMPARATOR = new AgeComparator();
    private static final Logger        log            = LoggerFactory.getLogger(CausalBuffer.class);

    private final IntCausalClock                                        clock;
    private final StampedClockValueComparator<Integer, IntStampedClock> comparator;
    private final ConcurrentMap<Digest, StampedMessage>                 delivered         = new ConcurrentHashMap<>();
    private final Consumer<Map<Digest, List<StampedMessage>>>           delivery;
    private final Semaphore                                             garbageCollecting = new Semaphore(1);
    private final int                                                   maxAge;
    private final Parameters                                            params;
    private final AtomicReference<Digest>                               previous          = new AtomicReference<>();
    private final AtomicInteger                                         round             = new AtomicInteger();
    private final AtomicInteger                                         sequenceNumber    = new AtomicInteger();
    private final AtomicInteger                                         size              = new AtomicInteger();
    private final ConcurrentMap<Digest, Stream>                         streams           = new ConcurrentHashMap<>();
    private final Semaphore                                             tickGate          = new Semaphore(1);

    public CausalBuffer(Parameters parameters, Consumer<Map<Digest, List<StampedMessage>>> delivery) {
        this.params = parameters;
        this.clock = new IntCausalClock(new BloomClock(params.clockK, params.clockM), sequenceNumber,
                                        new ReentrantLock());
        this.delivery = delivery;
        this.maxAge = params.context.timeToLive();
        comparator = new StampedClockValueComparator<>(Hash.fpp(params.clockK, params.clockM, params.eventWindow));
        initPrevious();
    }

    public void clear() {
        round.set(0);
        delivered.clear();
        initPrevious();
        size.set(0);
        streams.clear();
        clock.reset();
    }

    record StreamedSM(Stream stream, StampedMessage sm) {}

    record Delivered(Stream stream, List<StampedMessage> delivered) {}

    public void deliver(List<CausalMessage> messages) {
        if (messages.size() == 0) {
            return;
        }
        log.trace("delivering {} msgs on: {}", messages.size(), params.member);
        observe(messages.stream()
                        .map(cm -> new StampedMessage(new Digest(cm.getSource()), new Digest(cm.getHash()),
                                                      IntStampedClockValue.from(cm.getClock()),
                                                      CausalMessage.newBuilder(cm)))
                        .filter(f -> !dup(f.hash, f.message.getAge())).map(sm -> process(sm, streamOf(sm)))
                        .filter(e -> e != null).peek(sm -> size.incrementAndGet())
                        .peek(ssm -> delivered.put(ssm.sm.hash(), ssm.sm))
                        .map(ssm -> streams.values().stream().parallel()
                                           .map(stream -> stream.observe(comparator, ssm.sm)))
                        .flatMap(e -> e).filter(e -> e != null)
                        .collect(Collectors.groupingBy(delivered -> delivered.stream.from, () -> new HashMap<>(),
                                                       Collectors.flatMapping(d -> d.delivered.stream().sorted(),
                                                                              Collectors.toList()))));
        gc();
    }

    public DigestBloomFilter forReconcilliation(DigestBloomFilter biff) {
        streams.values().forEach(stream -> stream.forReconcilliation(biff));
        delivered.values().forEach(received -> biff.add(received.hash()));

        return biff;
    }

    public List<CausalMessage> reconcile(BloomFilter<Digest> biff) {
        Queue<CausalMessage> mailBin = new PriorityQueue<>(AGE_COMPARATOR);
        delivered.values().stream().filter(r -> !biff.contains(r.hash())).filter(m -> m.message.getAge() < maxAge)
                 .forEach(m -> mailBin.add(m.message.build()));
        streams.values().forEach(stream -> stream.reconcile(biff, mailBin, maxAge));
        List<CausalMessage> reconciled = new ArrayList<>();
        while (!mailBin.isEmpty() && reconciled.size() < params.maxMessages) {
            reconciled.add(mailBin.poll());
        }
        return reconciled;
    }

    public int round() {
        return round.get();
    }

    public StampedMessage send(Any content, SigningMember member) {
        Digest prev = previous.get();
        Digest hash = params.digestAlgorithm.digest(prev.toDigeste().toByteString(), content.toByteString(),
                                                    member.getId().toDigeste().toByteString());

        IntStampedClock stamp = clock.stamp(hash);

        Sig sig = member.sign(hash.toDigeste().toByteString(), stamp.toByteString()).toSig();
        CausalMessage.Builder message = CausalMessage.newBuilder().setAge(0).setSource(member.getId().toDigeste())
                                                     .setClock(stamp).setContent(content).setHash(hash.toDigeste())
                                                     .setSignature(sig);
        StampedMessage stamped = new StampedMessage(member.getId(), hash, IntStampedClockValue.from(stamp), message);
        previous.set(hash);
        log.trace("Send message:{} on: {}", hash, params.member);
        deliver(Collections.singletonList(stamped.message.build()));
        return stamped;
    }

    public int size() {
        return size.get();
    }

    public void tick() {
        round.incrementAndGet();
        params.executor.execute(Utils.wrapped(() -> {
            try {
                if (!tickGate.tryAcquire(500, TimeUnit.MILLISECONDS)) {
                    log.error("Unable to acquire tick gate for: {} on: {}", params.context.getId(), params.member);
                    return;
                }
            } catch (InterruptedException e) {
                log.error("Unable to acquire tick gate for: {} on: {}", params.context.getId(), params.member, e);
            }
            try {
                streams.values().forEach(stream -> size.addAndGet(-stream.updateAge(maxAge)));
                var trav = delivered.entrySet().iterator();
                while (trav.hasNext()) {
                    var next = trav.next().getValue();
                    if (next.message.getAge() >= maxAge) {
                        log.trace("GC'ing: {} from: {} as buffer too full: {} > {} on: {}", next.hash, next.from,
                                  size.get(), params.bufferSize, params.member);
                        trav.remove();
                        size.decrementAndGet();
                    } else {
                        next.message.setAge(next.message.getAge() + 1);
                    }
                }
            } finally {
                tickGate.release();
            }
        }, log));
    }

    private boolean dup(Digest hash, int age) {
        if (age > maxAge) {
            log.trace("Rejecting message too old: {} age: {} on: {}", hash, age, params.member);
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
            log.debug("duplicate event: {} on: {}", hash, params.member);
            return true;
        }
        return false;
    }

    private void gc() {
        if (size.get() < params.bufferSize) {
            return;
        }
        if (!garbageCollecting.tryAcquire()) {
            return;
        }
        params.executor.execute(Utils.wrapped(() -> {
            try {
                int bufferSize = size.get();
                if (bufferSize < params.bufferSize) {
                    return;
                }
                log.trace("Compacting buffer: {} size: {} on: {}", params.context.getId(), bufferSize, params.member);
                purgeTheAged();
                int currentSize = size.get();
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
        log.debug("GC'ing: {} as buffer too full: {} > {} on: {}", params.context.getId(), size.get(),
                  params.bufferSize, params.member);
        while (processing.hasNext() && params.bufferSize < size.get()) {
            var m = processing.next();
            if (m.message.getAge() > maxAge) {
                delivered.remove(m.hash);
                log.trace("GC'ing: {} as buffer too full: {} > {} on: {}", m.hash, size.get(), params.bufferSize,
                          params.member);
                size.decrementAndGet();
            }
        }
    }

    private void gcPending() {
        log.debug("GC'ing pending messages on: {} as buffer too full: {} > {} on: {}", params.context.getId(),
                  size.get(), params.bufferSize, params.member);
        boolean gcd;
        while (params.bufferSize > size.get()) {
            gcd = false;
            for (Stream stream : streams.values()) {
                int trimmed = stream.trimAged();
                if (trimmed > 0) {
                    gcd = true;
                    size.addAndGet(-trimmed);
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

    private void observe(Map<Digest, List<StampedMessage>> mail) {
        if (!mail.isEmpty()) {
            delivery.accept(mail);
            if (log.isTraceEnabled()) {
                log.trace("Delivered: {} msgs for context: {} on: {} ",
                          mail.values().stream().flatMap(msgs -> msgs.stream()).count(), params.context.getId(),
                          params.member);
            }
        }
    }

    private StreamedSM process(StampedMessage candidate, Stream stream) {
        StampedMessage found = locked(() -> stream.queue().get(candidate), stream.lock);
        if (found == null) {
            if (comparator.compare(candidate.clock, clock) > 0) {
                log.debug("Rejecting stale event: {} from: {} on: {}", candidate.hash, stream.from(), params.member);
                return null;
            }
            if (verify(candidate.message, params.context.getActiveMember(candidate.from))) {
                locked(() -> stream.queue().put(candidate, candidate), stream.lock);
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
            size.decrementAndGet();
            locked(() -> stream.queue.remove(candidate), stream.lock());
        } else if (age != nextAge) {
            found.message().setAge(nextAge);
        }
        return null;
    }

    private void purgeTheAged() {
        log.debug("Purging the aged of: {} buffer size: {} delivered: {} on: {}", params.context.getId(), size.get(),
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
                size.decrementAndGet();
            } else {
                break;
            }
        }
        streams.values().forEach(stream -> size.addAndGet(-stream.purgeTheAged(maxAge, params.member.getId())));
        if (params.bufferSize < size.get()) {
            gcDelivered(processing);
        }
        if (params.bufferSize < size.get()) {
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
                                  new TreeMap<>(), new ReentrantLock());
            }
            return v;
        });

    }

    private boolean verify(CausalMessageOrBuilder message, Member from) {
        return from.verify(new JohnHancock(message.getSignature()), message.getHash().toByteString(),
                           message.getClock().toByteString());
    }
}
