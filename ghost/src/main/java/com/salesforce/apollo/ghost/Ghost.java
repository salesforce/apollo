/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import static com.salesforce.apollo.ghost.communications.GhostClientCommunications.getCreate;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Any;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Entry;
import com.salesfoce.apollo.ghost.proto.Get;
import com.salesfoce.apollo.ghost.proto.Intervals;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RingCommunications.Direction;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ghost.communications.GhostServerCommunications;
import com.salesforce.apollo.ghost.communications.SpaceGhost;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Spaaaaaaaaaaaace Ghooooooooossssssstttttt.
 * <p>
 * A distributed, content addresssable hash table. Keys of this DHT are the hash
 * of the content's bytes.
 * <p>
 * Builds on the Fireflies membership gossip service (and swiss army knife) to
 * implement a one hop imutable DHT. Stored content is only addressible by the
 * hash of the content. Thus, content is immutable (although we allow deletes,
 * because GC).
 * <p>
 * Ghost reuses the t+1 rings of the Fireflies context as the redundant storage
 * rings for content. The hash keys of the content map to each ring differently,
 * and so each Ghost instance stores t+1 intervals - perhaps overlapping - of
 * the current content set of the system wide DHT.
 * <p>
 * Content is stored redundantly on t+1 rings and Ghost emits n (where n <= t+1)
 * parallel communications for key lookup. If the key is stored, the first
 * responder with verified (hash(content) == lookup key) of the parallel query
 * is returned. If the key is not present, the client only waits for the
 * indicated timeout, rather than the sum of timeouts from t+1 serial queries.
 * <p>
 * Content storage operations must complete a majority of writes out of t+1
 * rings to return without error. As the key of any content is its hash, content
 * is immutable, so any put() operation may be retried, as put() is idempotent.
 * Note that idempotent push() does not mean zero overhead for redundant pushes.
 * There still will be communication overhead of at least the majority of ghost
 * nodes on the various rings.
 * <p>
 * To compensate for the wild, wild west of dynamic membership, Ghost gossips
 * with its n successors on each of the t+1 rings of the Fireflies context.
 * Because all content is stored redundantly, all lookups for validated,
 * previously stored content will be available whp, assuming non catastrophic
 * loss/gain in membership. The reuse of the t+1 rings of the underlying FF
 * context for storage redundancy sets the upper bounds on the "catastrophic"
 * cardinality and allows Ghost to update the storage for dynamic rebalancing
 * during membership changes. As long as at least 1 of the t+1 members remain as
 * the storage node during the context membership change on a querying node, the
 * DHT will return the result.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Ghost {

    public static class GhostParameters {
        public static class Builder {
            private DigestAlgorithm digestAlgorithm = DigestAlgorithm.DEFAULT;
            private Executor        executor        = ForkJoinPool.commonPool();
            private double          fpr;
            private int             maxEntries      = 100;

            public GhostParameters build() {
                return new GhostParameters(digestAlgorithm, executor, fpr, maxEntries);
            }

            public DigestAlgorithm getDigestAlgorithm() {
                return digestAlgorithm;
            }

            public Executor getExecutor() {
                return executor;
            }

            public double getFpr() {
                return fpr;
            }

            public int getMaxEntries() {
                return maxEntries;
            }

            public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
                this.digestAlgorithm = digestAlgorithm;
                return this;
            }

            public Builder setExecutor(Executor executor) {
                this.executor = executor;
                return this;
            }

            public Builder setFpr(double fpr) {
                this.fpr = fpr;
                return this;
            }

            public Builder setMaxEntries(int maxEntries) {
                this.maxEntries = maxEntries;
                return this;
            }
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public final DigestAlgorithm digestAlgorithm;
        public final Executor        executor;
        public final double          fpr;

        public final int maxEntries;

        public GhostParameters(DigestAlgorithm digestAlgorithm, Executor executor, double fpr, int maxEntries) {
            this.digestAlgorithm = digestAlgorithm;
            this.executor = executor;
            this.fpr = fpr;
            this.maxEntries = maxEntries;
        }
    }

    public class Service {

        public Any get(Get get) {
            return store.get(Digest.from(get.getId()));
        }

        public Entries intervals(Intervals request, Digest from) {
            if (from == null) {
                log.info("Intervals gossip from unknown member on: {}", member);
                return Entries.getDefaultInstance();
            }
            Member m = context.getActiveMember(from);
            if (m == null) {
                log.info("Intervals gossip from unknown member: {} on: {}", from, member);
                return Entries.getDefaultInstance();
            }
            Member successor = context.ring(request.getRing()).successor(member);
            if (!successor.equals(m)) {
                log.info("Invalid intervals gossip on ring: {} expecting: {} from: {} on: {}", request.getRing(),
                         successor, from, member);
                return Entries.getDefaultInstance();
            }
            log.trace("Intervals gossip from: {} on: {}", from, member);
            return store.entriesIn(new CombinedIntervals(
                    request.getIntervalsList().stream().map(e -> new KeyInterval(e)).collect(Collectors.toList())),
                                   parameters.maxEntries);
        }

        public void put(Entry entry) {
            Any value = entry.getValue();
            store.put(parameters.digestAlgorithm.digest(value.toByteString()), value);
        }
    }

    public static final int JOIN_MESSAGE_CHANNEL = 3;

    private static final Logger                             log     = LoggerFactory.getLogger(Ghost.class);
    private final CommonCommunications<SpaceGhost, Service> communications;
    private final Context<Member>                           context;
    private final RingCommunications<SpaceGhost>            gossiper;
    private final SigningMember                             member;
    private final GhostParameters                           parameters;
    private final Service                                   service = new Service();
    private final AtomicBoolean                             started = new AtomicBoolean();
    private final Store                                     store;

    public Ghost(SigningMember member, GhostParameters p, Router c, Context<Member> context, Store s) {
        this.member = member;
        parameters = p;
        this.context = context;
        store = s;
        SpaceGhost localLoopback = new SpaceGhost() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public ListenableFuture<Any> get(Get key) {
                SettableFuture<Any> f = SettableFuture.create();
                f.set(service.get(key));
                return f;
            }

            @Override
            public Member getMember() {
                return member;
            }

            @Override
            public ListenableFuture<Entries> intervals(Intervals intervals) {
                return null;
            }

            @Override
            public void put(Entry value) {
                service.put(value);
            }
        };
        communications = c.create(member, context.getId(), service,
                                  r -> new GhostServerCommunications(c.getClientIdentityProvider(), r), getCreate(),
                                  localLoopback);
        gossiper = new RingCommunications<>(Direction.PREDECESSOR, context, member, communications,
                parameters.executor);
    }

    /**
     * Answer the value associated with the key
     * 
     * @param key
     * @return the value associated with ye key
     */
    public Any get(Digest key, Duration timeout) throws TimeoutException {
        log.trace("Starting Get {}   on: {}", key, member);
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        CompletableFuture<Any> result = new CompletableFuture<>();
        Get get = Get.newBuilder().setContext(context.getId().toByteString()).setId(key.toByteString()).build();
        new RingCommunications<>(context, member, communications,
                parameters.executor).iterate(key, (link, r) -> link.get(get),
                                             (tally, futureSailor, link, r) -> get(futureSailor, key, result,
                                                                                   isTimedOut, link));
        try {
            return result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            TimeoutException t = new TimeoutException("Interrupted");
            t.initCause(e);
            throw t;
        } catch (ExecutionException e) {
            TimeoutException t = new TimeoutException("Execution error: " + e.getLocalizedMessage());
            t.initCause(e);
            throw t;
        }
    }

    public Member getMember() {
        return member;
    }

    /**
     * @return the network service singleton
     */
    public Service getService() {
        return service;
    }

    public CombinedIntervals keyIntervals() {
        List<KeyInterval> intervals = new ArrayList<>();
        for (int i = 0; i < context.getRingCount(); i++) {
            Ring<Member> ring = context.ring(i);
            Member predecessor = ring.predecessor(member);
            if (predecessor == null) {
                continue;
            }

            Digest begin = ring.hash(predecessor);
            Digest end = ring.hash(member);

            if (begin.compareTo(end) > 0) { // wrap around the origin of the ring
                intervals.add(new KeyInterval(end, parameters.digestAlgorithm.getLast()));
                intervals.add(new KeyInterval(parameters.digestAlgorithm.getOrigin(), begin));
            } else {
                intervals.add(new KeyInterval(begin, end));
            }
        }
        return new CombinedIntervals(intervals);
    }

    /**
     * Insert the value into the Ghost DHT. Return when a majority of rings have
     * stored the value
     * 
     * @param value
     * @return - the Digest of the value
     * @throws TimeoutException
     */
    public Digest put(Any value, Duration timeout) throws TimeoutException {
        Digest key = parameters.digestAlgorithm.digest(value.toByteString());
        log.trace("Starting Put {}   on: {}", key, member);

        CompletableFuture<Boolean> majority = new CompletableFuture<>();
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        Entry entry = Entry.newBuilder().setContext(context.getId().toByteString()).setValue(value).build();

        new RingCommunications<>(Direction.SUCCESSOR, context, member, communications,
                parameters.executor).iterate(key, () -> majorityComplete(key, majority),
                                             (link, r) -> put(link, key, entry), () -> failedMajority(key, majority),
                                             (tally, futureSailor, link, r) -> put(futureSailor, isTimedOut, key, tally,
                                                                                   link),
                                             null);

        try {
            Boolean completed = majority.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (completed != null && completed) {
                log.info("Successful put: {}  on: {}", key, member);
                return key;
            } else {
                throw new TimeoutException("Partial or complete failure to store: " + key);
            }
        } catch (InterruptedException e) {
            TimeoutException timeoutException = new TimeoutException("Interrupted");
            timeoutException.initCause(e);
            throw timeoutException;
        } catch (ExecutionException e) {
            TimeoutException timeoutException = new TimeoutException("Error");
            timeoutException.initCause(e);
            throw timeoutException;
        }
    }

    public void start(ScheduledExecutorService scheduler, Duration duration) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        communications.register(context.getId(), service);
        gossip(scheduler, duration);
    }

    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }
        communications.deregister(context.getId());
    }

    private void failedMajority(Digest key, CompletableFuture<Boolean> majority) {
        majority.complete(false);
        log.trace("Failed majority put: {}  on: {}", key, member);
    }

    private boolean get(Optional<ListenableFuture<Any>> futureSailor, Digest key, CompletableFuture<Any> result,
                        Supplier<Boolean> isTimedOut, SpaceGhost link) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        Any value;
        try {
            value = futureSailor.get().get();
        } catch (InterruptedException e) {
            log.trace("Error fetching: {} from: {} on: {}", key, link.getMember(), member, e);
            return !isTimedOut.get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException) t;
                if (sre.getStatus() == Status.NOT_FOUND) {
                    log.trace("Error fetching: {} server not found: {} on: {}", key, link.getMember(), member);
                    return !isTimedOut.get();
                }
            }
            log.trace("Error fetching: {} from: {} on: {}", key, link.getMember(), member, e.getCause());
            return !isTimedOut.get();
        }
        if (value != null) {
            log.trace("Get: {} from: {}  on: {}", key, link.getMember(), member);
            result.complete(value);
            return false;
        } else {
            log.trace("Failed get: {} from: {}  on: {}", key, link.getMember(), member);
            return !isTimedOut.get();
        }
    }

    private void gossip(Optional<ListenableFuture<Entries>> futureSailor, SpaceGhost link,
                        ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get()) {
            return;
        }
        if (futureSailor.isEmpty()) {
            return;
        }
        try {
            Entries entries = futureSailor.get().get();
            if (entries.getRecordsCount() > 0) {
                log.info("Received: {} entries in Ghost gossip from: {} on: {}", entries.getRecordsCount(),
                         link.getMember(), member);
            }
            store.add(entries.getRecordsList());
        } catch (InterruptedException | ExecutionException e) {
            log.debug("Error interval gossiping with {} : {}", link.getMember(), e.getCause());
        }
        if (started.get()) {
            scheduler.schedule(() -> gossip(scheduler, duration), duration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void gossip(ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get()) {
            return;
        }
        gossiper.execute((link, ring) -> gossip(link, ring),
                         (futureSailor, link, ring) -> gossip(futureSailor, link, scheduler, duration));

    }

    private ListenableFuture<Entries> gossip(SpaceGhost link, Integer ring) {
        CombinedIntervals keyIntervals = keyIntervals();
        log.trace("Starting one round of Ghost gossip on: {} intervals: {}", member, keyIntervals);
        store.populate(keyIntervals, parameters.fpr, Utils.secureEntropy());
        return link.intervals(Intervals.newBuilder()
                                       .setContext(context.getId().toByteString())
                                       .setRing(ring)
                                       .addAllIntervals(keyIntervals.toIntervals())
                                       .build());
    }

    private void majorityComplete(Digest key, CompletableFuture<Boolean> majority) {
        majority.complete(true);
        log.trace("Majority put {} on: {}", key, member);
    }

    private boolean put(Optional<ListenableFuture<Boolean>> futureSailor, Supplier<Boolean> isTimedOut, Digest key,
                        AtomicInteger tally, SpaceGhost link) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        try {
            futureSailor.get().get();
        } catch (InterruptedException e) {
            log.debug("Error fetching: {} from: {} on: {}", key, link.getMember(), member, e);
            return !isTimedOut.get();
        } catch (ExecutionException e) {
            log.debug("Error fetching: {} from: {} on: {}", key, link.getMember(), member, e.getCause());
            return !isTimedOut.get();
        }
        log.trace("Inc put {} on: {}", key, member);
        tally.incrementAndGet();
        return !isTimedOut.get();
    }

    private ListenableFuture<Boolean> put(SpaceGhost link, Digest key, Entry entry) {
        link.put(entry);
        log.trace("Put {} to: {} on: {}", key, link.getMember(), member);
        SettableFuture<Boolean> f = SettableFuture.create();
        f.set(true);
        return f;
    }
}
