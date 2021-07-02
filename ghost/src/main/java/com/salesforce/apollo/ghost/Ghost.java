/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import static com.salesforce.apollo.ghost.communications.GhostClientCommunications.getCreate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.salesfoce.apollo.ghost.proto.Bind;
import com.salesfoce.apollo.ghost.proto.Binding;
import com.salesfoce.apollo.ghost.proto.Content;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Entry;
import com.salesfoce.apollo.ghost.proto.Get;
import com.salesfoce.apollo.ghost.proto.Intervals;
import com.salesfoce.apollo.ghost.proto.Lookup;
import com.salesfoce.apollo.utils.proto.StampedClock;
import com.salesforce.apollo.comm.RingCommunications;
import com.salesforce.apollo.comm.RingIterator;
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
import com.salesforce.apollo.utils.bloomFilters.BloomClock;
import com.salesforce.apollo.utils.bloomFilters.ClockValue;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Spaaaaaaaaaaaace Ghooooooooossssssstttttt.
 * <p>
 * A distributed, content addresssable hash table. Keys of this DHT are the hash
 * of the content's bytes.
 * <p>
 * Builds on the membership gossip service (and swiss army knife) to implement a
 * one hop imutable DHT with eventually consistent mutable bindings. Stored
 * content is only addressible by the hash of the content. Thus, content is
 * immutable (although we allow deletes, because GC). Mutable bindings are
 * eventually consistent
 * <p>
 * Ghost reuses the t+1 rings of the Memberships context as the redundant
 * storage rings for content. The hash keys of the content map to each ring
 * differently, and so each Ghost instance stores t+1 intervals - perhaps
 * overlapping - of the current content set of the system wide DHT.
 * <p>
 * Content is stored redundantly on t+1 rings and Ghost emits n (where n <= t+1)
 * parallel communications for key lookup. If the key is stored, the first
 * responder with verified (hash(content) == lookup key) of the parallel query
 * is returned. If the key is not present, the client only waits for the
 * indicated timeout, rather than the sum of timeouts from t+1 serial queries.
 * <p>
 * Immutable ontent storage operations must complete a majority of writes out of
 * t+1 rings to return without error. As the key of any immutable content is its
 * hash, content is immutable, so any put() operation may be retried, as put()
 * is idempotent. Note that idempotent push() does not mean zero overhead for
 * redundant pushes. There still will be communication overhead of at least the
 * majority of ghost nodes on the various rings.
 * <p>
 * To compensate for the wild, wild west of dynamic membership, Ghost gossips
 * with its n successors on each of the t+1 rings of the context. Because all
 * content is stored redundantly, all lookups for validated, previously stored
 * content will be available whp, assuming non catastrophic loss/gain in
 * membership. The reuse of the t+1 rings of the underlying context for storage
 * redundancy sets the upper bounds on the "catastrophic" cardinality and allows
 * Ghost to update the storage for dynamic rebalancing during membership
 * changes. As long as at least 1 of the t+1 members remain as the storage node
 * for a given content hash key during the context membership change on a
 * querying node, the DHT will return the result.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Ghost {

    public static class GhostParameters {
        public static class Builder {
            private DigestAlgorithm digestAlgorithm = DigestAlgorithm.DEFAULT;
            private Executor        executor        = ForkJoinPool.commonPool();
            private double          fpr             = 0.00125;
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
        public final int             maxEntries;

        public GhostParameters(DigestAlgorithm digestAlgorithm, Executor executor, double fpr, int maxEntries) {
            this.digestAlgorithm = digestAlgorithm;
            this.executor = executor;
            this.fpr = fpr;
            this.maxEntries = maxEntries;
        }
    }

    public class Service {

        public void bind(Bind bind) {
            Binding binding = bind.getBinding();
            Digest key = parameters.digestAlgorithm.digest(binding.getKey());
            store.bind(key, binding);
            log.trace("Bind: {} on: {}", key, member);
        }

        public Content get(Get get) {
            Digest key = Digest.from(get.getCid());
            Content content = store.get(key);
            log.trace("Get: {} non NULL: {} on: {}", key, content != null, member);
            return content;
        }

        public Entries intervals(Intervals request, Digest from) {
            if (from == null) {
                log.warn("Intervals gossip from unknown member on: {}", member);
                return Entries.getDefaultInstance();
            }
            Member m = context.getActiveMember(from);
            if (m == null) {
                log.warn("Intervals gossip from unknown member: {} on: {}", from, member);
                return Entries.getDefaultInstance();
            }
            Member predecessor = context.ring(request.getRing()).predecessor(member);
            if (!predecessor.equals(m)) {
                log.warn("Invalid intervals gossip on ring: {} expecting: {} from: {} on: {}", request.getRing(),
                         predecessor, from, member);
                return Entries.getDefaultInstance();
            }
            log.trace("Intervals gossip from: {} on: {}", from, member);
            return store.entriesIn(new CombinedIntervals(
                    request.getIntervalsList().stream().map(e -> new KeyInterval(e)).collect(Collectors.toList())),
                                   parameters.maxEntries);
        }

        public Binding lookup(Lookup query) {
            Binding binding = store.lookup(Digest.from(query.getKey()));
            log.trace("Lookup: {} non NULL: {} on: {}", query.getKey(), binding != null, member);
            return binding;
        }

        public void purge(Get get) {
            Digest key = new Digest(get.getCid());
            store.purge(key);
            log.trace("Purge: {} on: {}", key, member);
        }

        public void put(Entry entry) {
            Content content = entry.getContent();
            Digest cid = parameters.digestAlgorithm.digest(content.getValue().toByteString());
            store.put(cid, content);
            log.trace("Put: {} on: {}", cid, member);
        }

        public void remove(Lookup query) {
            store.remove(Digest.from(query.getKey()));
            log.trace("Remove: {} on: {}", query.getKey(), member);
        }
    }

    record CausalityClock(Lock lock, BloomClock clock, java.time.Clock wallClock) {

        ClockValue current() {
            return locked(() -> clock.current());
        }

        ClockValue merge(ClockValue b) {
            return locked(() -> clock.merge(b));
        }

        ClockValue merge(StampedClock b) {
            return merge(ClockValue.of(b.getClock()));
        }

        StampedClock.Builder stamp(Digest digest) {
            return locked(() -> {
                clock.add(digest);
                Instant now = wallClock.instant();
                return StampedClock.newBuilder()
                                   .setClock(clock.toClock())
                                   .setStamp(Timestamp.newBuilder()
                                                      .setSeconds(now.getEpochSecond())
                                                      .setNanos(now.getNano()));
            });

        }

        private <T> T locked(Callable<T> call) {
            lock.lock();
            try {
                return call.call();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    public static final int JOIN_MESSAGE_CHANNEL = 3;

    private static final Logger                             log     = LoggerFactory.getLogger(Ghost.class);
    private final CausalityClock                            clock;
    private final CommonCommunications<SpaceGhost, Service> communications;
    private final Context<Member>                           context;
    private final RingCommunications<SpaceGhost>            gossiper;
    private final SigningMember                             member;
    private final GhostParameters                           parameters;
    private final Service                                   service = new Service();
    private final AtomicBoolean                             started = new AtomicBoolean();
    private final Store                                     store;

    public Ghost(SigningMember member, GhostParameters p, Router c, Context<Member> context, MVStore store) {
        this(member, p, c, context, store, new BloomClock());
    }

    public Ghost(SigningMember member, GhostParameters p, Router c, Context<Member> context, MVStore store,
            BloomClock clock) {
        this(member, p, c, context, new GhostStore(context.getId(), p.digestAlgorithm, store), clock,
                java.time.Clock.systemUTC());
    }

    public Ghost(SigningMember member, GhostParameters p, Router c, Context<Member> context, Store s, BloomClock clock,
            java.time.Clock wallClock) {
        this.member = member;
        parameters = p;
        this.context = context;
        store = s;
        this.clock = new CausalityClock(new ReentrantLock(), clock, wallClock);
        communications = c.create(member, context.getId(), service,
                                  r -> new GhostServerCommunications(c.getClientIdentityProvider(), r), getCreate(),
                                  SpaceGhost.localLoopbackFor(member, service));
        gossiper = new RingCommunications<>(context, member, communications, parameters.executor);
    }

    /**
     * Bind the value with the key. This is an updatable operation with different
     * values for the same key.
     */
    public void bind(String key, Any value, Duration timeout) throws TimeoutException {
        Digest hash = parameters.digestAlgorithm.digest(key);
        log.trace("Starting Put {}   on: {}", key, member);

        CompletableFuture<Boolean> majority = new CompletableFuture<>();
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);

        // Bind the key and value at the current Bloom Clock value and wall clock
        // instant
        Binding binding = Binding.newBuilder()
                                 .setKey(key)
                                 .setValue(value)
                                 .setClock(clock.stamp(parameters.digestAlgorithm.digest(value.toByteString())))
                                 .build();

        new RingIterator<>(context, member, communications,
                parameters.executor).iterate(hash, () -> majorityComplete(key, majority),
                                             (link, r) -> bind(link, key, binding), () -> failedMajority(key, majority),
                                             (tally, futureSailor, link, r) -> bind(futureSailor, isTimedOut, key,
                                                                                    tally, link),
                                             null);

        try {
            Boolean completed = majority.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (completed != null && completed) {
                log.trace("Successful bind: {}  on: {}", key, member);
                return;
            } else {
                throw new TimeoutException("Partial or complete failure to bind: " + key);
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

    /**
     * Answer the value associated with the key
     * 
     * @param key
     * @return the value associated with ye key
     */
    public Optional<Content> get(Digest key, Duration timeout) throws TimeoutException {
        log.trace("Starting Get {}   on: {}", key, member);
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        CompletableFuture<Content> result = new CompletableFuture<>();
        Get get = Get.newBuilder().setContext(context.getId().toDigeste()).setCid(key.toDigeste()).build();
        new RingIterator<>(context, member, communications,
                parameters.executor).iterate(key, (link, r) -> link.get(get),
                                             (tally, futureSailor, link, r) -> get(futureSailor, key, result,
                                                                                   isTimedOut, link));
        try {
            return Optional.ofNullable(result.get(timeout.toMillis(), TimeUnit.MILLISECONDS));
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
     * Lookup the current value associated with the key
     */
    public Optional<Binding> lookup(String key, Duration timeout) throws TimeoutException {
        log.trace("Starting Lookup {}   on: {}", key, member);
        Digest hash = parameters.digestAlgorithm.digest(key);
        Instant timedOut = Instant.now().plus(timeout);
        Supplier<Boolean> isTimedOut = () -> Instant.now().isAfter(timedOut);
        CompletableFuture<Binding> result = new CompletableFuture<>();
        Lookup lookup = Lookup.newBuilder().setContext(context.getId().toDigeste()).setKey(hash.toDigeste()).build();
        Multiset<Binding> votes = HashMultiset.create();

        new RingIterator<>(context, member, communications,
                parameters.executor).iterate(hash, (link, r) -> link.lookup(lookup),
                                             (tally, futureSailor, link, r) -> lookup(futureSailor, key, votes, result,
                                                                                      isTimedOut, link),
                                             () -> result.completeExceptionally(new TimeoutException(
                                                     "Failed to acheieve majority aggrement for: " + key + " on: "
                                                             + context.getId() + " votes: "
                                                             + votes.stream()
                                                                    .map(a -> votes.count(a))
                                                                    .collect(Collectors.toList()))));
        try {
            return Optional.ofNullable(result.get(timeout.toMillis(), TimeUnit.MILLISECONDS));
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

        // Bind the content at current Bloom Clock value and wall clock instant
        Entry entry = Entry.newBuilder()
                           .setContext(context.getId().toDigeste())
                           .setContent(Content.newBuilder().setClock(clock.stamp(key)).setValue(value))
                           .build();

        new RingIterator<>(context, member, communications,
                parameters.executor).iterate(key, () -> majorityComplete(key, majority),
                                             (link, r) -> put(link, key, entry), () -> failedMajority(key, majority),
                                             (tally, futureSailor, link, r) -> put(futureSailor, isTimedOut, key, tally,
                                                                                   link),
                                             null);

        try {
            Boolean completed = majority.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (completed != null && completed) {
                log.trace("Successful put: {}  on: {}", key, member);
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

    private boolean bind(Optional<ListenableFuture<Empty>> futureSailor, Supplier<Boolean> isTimedOut, String key,
                         AtomicInteger tally, SpaceGhost link) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        try {
            futureSailor.get().get();
        } catch (InterruptedException e) {
            log.warn("Error binding: {} from: {} on: {}", key, link.getMember(), member, e);
            return !isTimedOut.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException) e.getCause();
                if (sre.getStatus() == Status.UNAVAILABLE) {
                    log.trace("Server unavailable binding: {} from: {} on: {}", key, link.getMember(), member);
                }
            } else {
                log.warn("Error binding: {} from: {} on: {}", key, link.getMember(), member, e.getCause());
            }
            return !isTimedOut.get();
        }
        var t = tally.incrementAndGet();
        log.trace("Inc bind {} tally: {} on: {}", key, t, member);
        return !isTimedOut.get();
    }

    private ListenableFuture<Empty> bind(SpaceGhost link, String key, Binding binding) {
        log.trace("Bind {} to: {} on: {}", key, link.getMember(), member);
        return link.bind(Bind.newBuilder().setContext(context.getId().toDigeste()).setBinding(binding).build());
    }

    private void failedMajority(Digest key, CompletableFuture<Boolean> majority) {
        majority.completeExceptionally(new TimeoutException(
                String.format("Failed majority put: %s  on: %s", key, member)));
        log.info("Failed majority put: {}  on: {}", key, member);
    }

    private void failedMajority(String key, CompletableFuture<Boolean> majority) {
        majority.completeExceptionally(new TimeoutException(
                String.format("Failed majority bind: %s  on: %s", key, member)));
        log.info("Failed majority bind: {}  on: {}", key, member);
    }

    private boolean get(Optional<ListenableFuture<Content>> futureSailor, Digest key, CompletableFuture<Content> result,
                        Supplier<Boolean> isTimedOut, SpaceGhost link) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        Content content;
        try {
            content = futureSailor.get().get();
        } catch (InterruptedException e) {
            log.debug("Error get: {} from: {} on: {}", key, link.getMember(), member, e);
            return !isTimedOut.get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException) t;
                if (sre.getStatus() == Status.NOT_FOUND) {
                    log.trace("Error get: {} server not found: {} on: {}", key, link.getMember(), member);
                    return !isTimedOut.get();
                }
            }
            log.debug("Error get: {} from: {} on: {}", key, link.getMember(), member, e.getCause());
            return !isTimedOut.get();
        }
        if (content != null || (content != null && content.equals(Content.getDefaultInstance()))) {
            log.trace("Get: {} from: {}  on: {}", key, link.getMember(), member);
            result.complete(content);
            return false;
        } else {
            log.debug("Failed get: {} from: {}  on: {}", key, link.getMember(), member);
            return !isTimedOut.get();
        }
    }

    private void gossip(Optional<ListenableFuture<Entries>> futureSailor, SpaceGhost link,
                        ScheduledExecutorService scheduler, Duration duration) {
        if (!started.get() || futureSailor.isEmpty()) {
            return;
        }
        try {
            Entries entries = futureSailor.get().get();
            if (entries.getContentCount() > 0 || entries.getBindingCount() > 0) {
                log.info("Received: {} immutable and {} mutable entries in Ghost gossip from: {} on: {}",
                         entries.getContentCount(), entries.getContentCount(), link.getMember(), member);
            } else if (log.isDebugEnabled()) {
                log.debug("Received: {} immutable and {} mutable entries in Ghost gossip from: {} on: {}",
                          entries.getContentCount(), entries.getBindingCount(), link.getMember(), member);
            }
            store.add(entries.getContentList());
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
        log.trace("Ghost gossip on ring: {} with: {} on: {} intervals: {}", ring, link.getMember(), member,
                  keyIntervals);
        store.populate(keyIntervals, parameters.fpr, Utils.secureEntropy());
        return link.intervals(Intervals.newBuilder()
                                       .setContext(context.getId().toDigeste())
                                       .setRing(ring)
                                       .addAllIntervals(keyIntervals.toIntervals())
                                       .build());
    }

    private boolean lookup(Optional<ListenableFuture<Binding>> futureSailor, String key, Multiset<Binding> votes,
                           CompletableFuture<Binding> result, Supplier<Boolean> isTimedOut, SpaceGhost link) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        Binding binding;
        try {
            binding = futureSailor.get().get();
        } catch (InterruptedException e) {
            log.debug("Error lookup: {} from: {} on: {}", key, link.getMember(), member, e);
            return !isTimedOut.get();
        } catch (ExecutionException e) {
            Throwable t = e.getCause();
            if (t instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException) t;
                if (sre.getStatus() == Status.NOT_FOUND) {
                    log.trace("Error lookup: {} server not found: {} on: {}", key, link.getMember(), member);
                    return !isTimedOut.get();
                }
            }
            log.debug("Error lookup: {} from: {} on: {}", key, link.getMember(), member, e.getCause());
            return !isTimedOut.get();
        }
        if (binding != null || (binding != null && binding.equals(Binding.getDefaultInstance()))) {
            log.trace("Lookup: {} from: {}  on: {}", key, link.getMember(), member);
            votes.add(binding);
            for (Binding vote : votes) {
                if (votes.count(vote) > context.majority()) {
                    result.complete(vote);
                    return false;
                }
            }
            return true;
        } else {
            log.debug("Failed lookup: {} from: {}  on: {}", key, link.getMember(), member);
            return !isTimedOut.get();
        }
    }

    private void majorityComplete(Digest key, CompletableFuture<Boolean> majority) {
        majority.complete(true);
        log.debug("Majority put: {} on: {}", key, member);
    }

    private void majorityComplete(String key, CompletableFuture<Boolean> majority) {
        majority.complete(true);
        log.debug("Majority bind: {} on: {}", key, member);
    }

    private boolean put(Optional<ListenableFuture<Empty>> futureSailor, Supplier<Boolean> isTimedOut, Digest key,
                        AtomicInteger tally, SpaceGhost link) {
        if (futureSailor.isEmpty()) {
            return !isTimedOut.get();
        }
        try {
            futureSailor.get().get();
        } catch (InterruptedException e) {
            log.warn("Error put: {} from: {} on: {}", key, link.getMember(), member, e);
            return !isTimedOut.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof StatusRuntimeException) {
                StatusRuntimeException sre = (StatusRuntimeException) e.getCause();
                if (sre.getStatus() == Status.UNAVAILABLE) {
                    log.trace("Server unavailable put: {} from: {} on: {}", key, link.getMember(), member);
                }
            } else {
                log.warn("Error put: {} from: {} on: {}", key, link.getMember(), member, e.getCause());
            }
            return !isTimedOut.get();
        }
        log.trace("Inc put {} on: {}", key, member);
        tally.incrementAndGet();
        return !isTimedOut.get();
    }

    private ListenableFuture<Empty> put(SpaceGhost link, Digest key, Entry entry) {
        log.trace("Put {} to: {} on: {}", key, link.getMember(), member);
        return link.put(entry);
    }
}
