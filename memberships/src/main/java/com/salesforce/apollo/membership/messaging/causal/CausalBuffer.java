/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.causal;

import static com.salesforce.apollo.utils.Utils.locked;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

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
import com.salesforce.apollo.utils.bc.BloomClock;
import com.salesforce.apollo.utils.bc.CausalityClock;
import com.salesforce.apollo.utils.bc.ClockValue;
import com.salesforce.apollo.utils.bc.TimeStampedClockValue;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;
import com.salesforce.apollo.utils.bloomFilters.Hash.DigestHasher;

/**
 * @author hal.hildebrand
 *
 */
public class CausalBuffer {
    public record StampedMessage(Digest hash, TimeStampedClockValue clock, Digest from, CausalMessage.Builder message)
                                implements Comparable<StampedMessage> {

        @Override
        public StampedMessage clone() {
            return new StampedMessage(hash, clock, from, message.clone());
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
            return clock.stamp().compareTo(o.clock.stamp());
        }
    }

    private static class AgeComparator implements Comparator<CausalMessage> {
        @Override
        public int compare(CausalMessage a, CausalMessage b) {
            return Integer.compare(a.getAge(), b.getAge());
        }
    }

    private record Filtering(Digest hash, Digest id, Instant sent, CausalMessage message) {}

    private record Stream(Digest id, BloomClock clock, TreeMap<StampedMessage, StampedMessage> queue,
                          Comparator<ClockValue> comparator, Lock lock) {

        List<StampedMessage> observe(Instant observed, List<Digest> digests) {
            return locked(() -> {
                clock.addAll(digests);
                List<StampedMessage> ready = new ArrayList<>();
                var trav = queue.entrySet().iterator();
                while (trav.hasNext()) {
                    var entry = trav.next();
                    var next = entry.getValue();
                    int compared = comparator.compare(next.clock(), clock);
                    if (compared < 0) {
                        ready.add(next);
                        trav.remove();
                    } else if (compared == 0) {
                        if (observed.isAfter(next.clock.stamp())) {
                            ready.add(next);
                            trav.remove();
                        } else {
                            return ready;
                        }
                    } else {
                        return ready;
                    }
                }
                return ready;
            }, lock);
        }

        void reconcile(BloomFilter<Digest> biff, Queue<CausalMessage> mailBin) {
            locked(() -> {
                queue.values().stream().filter(r -> !biff.contains(r.hash()))
                     .forEach(m -> mailBin.add(m.message().build()));
            }, lock);
        }

        void updateAge() {
            locked(() -> queue.values().forEach(s -> s.message().setAge(s.message().getAge() + 1)), lock);
        }

        void forReconcilliation(DigestBloomFilter biff) {
            locked(() -> queue.values().forEach(message -> biff.add(message.hash())), lock);
        }

        int purgeTheAged(int maxAge) {
            return locked(() -> {
                int purged = 0;
                var trav = queue.entrySet().iterator();
                while (trav.hasNext()) {
                    var m = trav.next().getValue();
                    if (m.message.getAge() > maxAge) {
                        purged++;
                        trav.remove();
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

    private final CausalityClock                              clock;
    private final ConcurrentMap<Digest, StampedMessage>       delivered         = new ConcurrentHashMap<>();
    private final Consumer<Map<Digest, List<StampedMessage>>> delivery;
    private final Semaphore                                   garbageCollecting = new Semaphore(1);
    private final int                                         maxAge;
    private final Parameters                                  params;
    private final AtomicReference<Digest>                     previous          = new AtomicReference<>();
    private final AtomicInteger                               round             = new AtomicInteger();
    private final AtomicInteger                               size              = new AtomicInteger();
    private final ConcurrentMap<Digest, Stream>               streams           = new ConcurrentHashMap<>();

    public CausalBuffer(Parameters parameters, Consumer<Map<Digest, List<StampedMessage>>> delivery) {
        this.params = parameters;
        this.clock = new CausalityClock(new BloomClock(seedFor(params.member.getId()), params.clockK, params.clockM),
                                        params.wallclock, new ReentrantLock());
        this.delivery = delivery;
        this.maxAge = params.context.timeToLive();
        initPrevious();
    }

    public void clear() {
        round.set(0);
        delivered.clear();
        initPrevious();
        size.set(0);
        streams.clear();
    }

    public void deliver(List<CausalMessage> messages) {
        Map<Digest, List<StampedMessage>> binned = new HashMap<>();
        messages.stream().filter(cm -> !(cm.getAge() > maxAge))
                .map(cm -> new Filtering(new Digest(cm.getHash()), new Digest(cm.getSource()), instantOf(cm), cm))
                .filter(f -> !f.sent.plus(params.tooOld).isBefore(clock.instant())).filter(f -> !dup(f)).forEach(f -> {
                    binned.computeIfAbsent(f.id, k -> {
                        Member from = params.context.getActiveMember(f.id);
                        if (from == null) {
                            return null;
                        }
                        return new ArrayList<>();
                    }).add(new StampedMessage(f.hash, TimeStampedClockValue.from(f.message.getClock()), f.id,
                                              CausalMessage.newBuilder(f.message)));
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
        Queue<CausalMessage> mailBin = new PriorityQueue<>(AGE_COMPARATOR);
        delivered.values().stream().filter(r -> !biff.contains(r.hash())).forEach(m -> mailBin.add(m.message.build()));
        streams.values().forEach(stream -> stream.reconcile(biff, mailBin));
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

        StampedClock stamp = clock.stamp(hash);

        Sig sig = member.sign(hash.toDigeste().toByteString(), stamp.toByteString()).toSig();
        CausalMessage.Builder message = CausalMessage.newBuilder().setAge(0).setSource(member.getId().toDigeste())
                                                     .setClock(stamp).setContent(content).setHash(hash.toDigeste())
                                                     .setSignature(sig);
        StampedMessage stamped = new StampedMessage(hash, TimeStampedClockValue.from(stamp), member.getId(), message);
        delivered.put(hash, stamped);
        size.incrementAndGet();
        previous.set(hash);
        log.trace("Send message:{} on: {}", hash, params.member);
        observe(Collections.singletonList(hash), params.wallclock.instant());
        return stamped;
    }

    public int size() {
        return size.get();
    }

    public void tick() {
        round.incrementAndGet();
        streams.values().forEach(stream -> stream.updateAge());
        delivered.values().forEach(r -> r.message().setAge(r.message().getAge() + 1));
    }

    private List<List<Digest>> deliver(Map<Digest, List<StampedMessage>> messages) {
        var accumulated = new ArrayList<List<Digest>>();
        for (Entry<Digest, List<StampedMessage>> entry : messages.entrySet()) {
            StampedMessage reset = entry.getValue().stream().filter(m -> m.message.getStreamReset()).findFirst()
                                        .orElse(null);
            if (reset != null) {// Reset of stream by the originating node
                var id = entry.getKey();
                streams.put(entry.getKey(),
                            new Stream(id,
                                       new BloomClock(seedFor(id), reset.clock.toClock(), params.clockK, params.clockM),
                                       new TreeMap<>(Collections.reverseOrder()), params.comparator,
                                       new ReentrantLock()));
                log.warn("Reset stream: {} event:{} on: {}", id, reset.hash, params.member);
            } else {
                var stream = streams.computeIfAbsent(entry.getKey(), id -> {
                    return new Stream(id,
                                      new BloomClock(seedFor(id), entry.getValue().get(0).clock.toClock(),
                                                     params.clockK, params.clockM),
                                      new TreeMap<>(Collections.reverseOrder()), params.comparator,
                                      new ReentrantLock());
                });
                var now = params.wallclock.instant();
                accumulated.add(deliver(stream, entry.getValue(), now));
            }
        }
        return accumulated;
    }

    private List<Digest> deliver(Stream stream, List<StampedMessage> messages, Instant now) {
        List<Digest> delivered = new ArrayList<>();
        for (StampedMessage candidate : messages) {
            StampedMessage found = locked(() -> stream.queue().get(candidate), stream.lock());
            if (found == null) {
                if (verify(candidate.message, params.context.getActiveMember(candidate.from))) {
                    delivered.add(candidate.hash);
                    locked(() -> stream.queue().put(candidate, candidate), stream.lock());
                    continue;
                } else {
                    log.debug("Rejecting: {} could not verify from: {} on: {}", candidate.hash, stream.id(),
                              params.member);
                    continue;
                }
            } else if (!candidate.message.getSignature().equals(found.message.getSignature())) {
                log.debug("Rejecting: {} as signature does match recorded from: {} on: {}", candidate.hash, stream.id(),
                          params.member);
                continue;
            }
            int age = candidate.message.getAge();
            int nextAge = Math.max(age, found.message.getAge());
            if (nextAge > maxAge || candidate.clock().stamp().isAfter(now)) {
                size.decrementAndGet();
                locked(() -> stream.queue.remove(candidate), stream.lock());
            } else if (age != nextAge) {
                found.message().setAge(nextAge);
            }
        }
        return delivered;
    }

    private boolean dup(Filtering f) {
        StampedMessage previous = delivered.get(f.hash);
        if (previous != null) {
            int nextAge = Math.max(previous.message().getAge(), f.message.getAge());
            if (nextAge > maxAge) {
                delivered.remove(f.hash);
            } else if (previous.message.getAge() != nextAge) {
                previous.message().setAge(nextAge);
            }
            return true;
        }
        return false;
    }

    private void gc() {
        if (!garbageCollecting.tryAcquire()) {
            return;
        }
        params.executor.execute(() -> {
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
                    log.trace("Buffer freed: {} after compact for: {} on: {} ", freed, params.context.getId(),
                              params.member);
                }
            } finally {
                garbageCollecting.release();
            }
        });

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
        Map<Digest, List<StampedMessage>> mail = new HashMap<>();
        streams.values().forEach(stream -> {
            var msgs = stream.observe(observed, sent).stream().peek(r -> delivered.put(r.hash(), r)).toList();
            if (!msgs.isEmpty()) {
                mail.put(stream.id(), msgs);
            }
        });

        if (!mail.isEmpty()) {
            delivery.accept(mail);
            if (log.isTraceEnabled()) {
                log.trace("Delivered: {} msgs context: {} on: {} ",
                          mail.values().stream().flatMap(msgs -> msgs.stream()).count(), params.context.getId(),
                          params.member);
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
        Queue<StampedMessage> processing = new PriorityQueue<>(Collections.reverseOrder((a,
                                                                                         b) -> Integer.compare(a.message.getAge(),
                                                                                                               b.message.getAge())));
        processing.addAll(delivered.values());
        var trav = processing.iterator();
        while (trav.hasNext()) {
            var m = trav.next();
            if (m.message.getAge() > maxAge) {
                delivered.remove(m.hash);
                size.decrementAndGet();
            } else {
                break;
            }
        }
        streams.values().forEach(stream -> size.addAndGet(-stream.purgeTheAged(maxAge)));

        while (trav.hasNext() && params.bufferSize < size.get()) {
            var m = trav.next();
            if (m.message.getAge() > maxAge) {
                delivered.remove(m.hash);
                log.debug("GC'ing: {} as buffer too full: {} > {} on: {}", m.hash, size.get(), params.bufferSize,
                          params.member);
                size.decrementAndGet();
            }
        }
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

    private long seedFor(Digest id) {
        DigestHasher hasher = new DigestHasher();
        hasher.establish(id, 0);
        hasher.process(params.context.getId());
        return hasher.getH1();
    }

    private boolean verify(CausalMessageOrBuilder message, Member from) {
        return from.verify(new JohnHancock(message.getSignature()), message.getHash().toByteString(),
                           message.getClock().toByteString());
    }
}
