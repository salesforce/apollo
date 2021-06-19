/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import static com.salesforce.apollo.ghost.communications.GhostClientCommunications.getCreate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.salesfoce.apollo.ghost.proto.Entries;
import com.salesfoce.apollo.ghost.proto.Interval;
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.ghost.communications.GhostClientCommunications;
import com.salesforce.apollo.ghost.communications.GhostServerCommunications;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.RingCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;

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
        public DigestAlgorithm digestAlgorithm = DigestAlgorithm.DEFAULT;
        public Executor        executor;
        public long            maxConnections  = 3;
        public int             redundancy      = 3;
        public long            timeout         = 30;
        public TimeUnit        unit            = TimeUnit.SECONDS;
    }

    /**
     * The network service interface
     */
    public class Service {
        private final AtomicBoolean started = new AtomicBoolean();

        public Any get(Digest key) {
            return store.get(key);
        }

        public List<Any> intervals(List<Interval> intervals, List<Digest> have) {
            return store.entriesIn(new CombinedIntervals(
                    intervals.stream().map(e -> new KeyInterval(e)).collect(Collectors.toList())), have);
        }

        public Void put(Any value) {
            store.put(parameters.digestAlgorithm.digest(value.toByteString()), value);
            return null;
        }

        public List<Any> satisfy(List<Digest> want) {
            return store.getUpdates(want);
        }

        public void start() {
            if (!started.compareAndSet(false, true)) {
                return;
            }
            communications.register(context.getId(), service);
        }

        public void stop() {
            if (!started.compareAndSet(true, false)) {
                return;
            }
            communications.deregister(context.getId());
        }
    }

    public static final int     JOIN_MESSAGE_CHANNEL = 3;
    private static final Logger log                  = LoggerFactory.getLogger(Ghost.class);

    private final CommonCommunications<GhostClientCommunications, Service> communications;
    private final Context<Member>                                          context;
    private final RingCommunications<GhostClientCommunications>                      gossiper; 
    private final SigningMember                                            member;
    private final GhostParameters                                          parameters;
    private final Service                                                  service = new Service();
    private final Store                                                    store;

    public Ghost(SigningMember member, GhostParameters p, Router c, Context<Member> context, Store s) {
        this.member = member;
        parameters = p;
        this.context = context;
        store = s;
        communications = c.create(member, context.getId(), service,
                                  r -> new GhostServerCommunications(c.getClientIdentityProvider(), r), getCreate());
        gossiper = new RingCommunications<>(context, member, communications, parameters.executor);
    }

    /**
     * Answer the Any associated with the key
     * 
     * @param key
     * @return the Any associated with ye key
     */
    public Any getDagEntry(Digest key) {
        CompletionService<Any> frist = new ExecutorCompletionService<>(parameters.executor);
        List<Future<Any>> futures = context.successors(key).stream().map(successor -> frist.submit(() -> {
            Any Any;
            GhostClientCommunications connection = communications.apply(successor, member);
            if (connection == null) {
                log.debug("Error looking up {} on {} connection is null", key, successor);
                return null;
            }
            try {
                Any = connection.get(key);
            } catch (Exception e) {
                log.debug("Error looking up {} on {} : {}", key, successor, e);
                return null;
            } finally {
                connection.release();
            }
            log.debug("on {} get {} from {} get: {}", member.getId(), key, successor.getId(), Any != null);
            return Any;
        })).collect(Collectors.toList());

        int retries = futures.size();
        long remainingTimout = parameters.unit.toMillis(parameters.timeout);

        try {
            Any Any = null;
            while (Any == null && --retries >= 0) {
                long then = System.currentTimeMillis();
                Future<Any> result = null;
                try {
                    result = frist.poll(remainingTimout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.debug("interrupted retrieving key: {}", key);
                    return null;
                }
                try {
                    Any = result.get();
                } catch (InterruptedException e) {
                    return null;
                } catch (ExecutionException e) {
                    log.debug("exception retrieving key: {}: {}", key, e);
                }
                remainingTimout = remainingTimout - (System.currentTimeMillis() - then);
            }
            return Any;
        } finally {
            futures.forEach(f -> f.cancel(true));
        }
    }

    /**
     * @return the network service singleton
     */
    public Service getService() {
        return service;
    } 

    /**
     * Insert the Any into the Ghost DHT. Return when a majority of rings have
     * stored the Any
     * 
     * @param Any
     * @return - the Digest of the Any
     */
    public Digest putDagEntry(Any Any) {
        Digest key = parameters.digestAlgorithm.digest(Any.toByteString());
        CompletionService<Boolean> frist = new ExecutorCompletionService<>(parameters.executor);
        List<Future<Boolean>> futures = context.successors(key).stream().map(successor -> frist.submit(() -> {
            if (successor != null) {
                GhostClientCommunications connection = communications.apply(successor, member);
                if (connection != null) {
                    try {
                        connection.put(Any);
                        log.debug("put {} on {}", key, successor.getId());
                        return true;
                    } finally {
                        connection.release();
                    }
                } else {
                    log.debug("Error inserting {} on {} no connection", key, successor);
                }
            }

            return false;
        })).collect(Collectors.toList());
        int retries = futures.size();
        long remainingTimout = parameters.unit.toMillis(parameters.timeout);

        int remainingAcks = context.getRingCount();

        while (--retries >= 0) {
            long then = System.currentTimeMillis();
            Future<Boolean> result = null;
            try {
                result = frist.poll(remainingTimout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.debug("interrupted retrieving key: {}", key);
                continue;
            }
            try {
                if (result.get()) {
                    remainingAcks--;
                    if (remainingAcks <= 0) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                continue;
            } catch (ExecutionException e) {
                log.debug("exception puting key: {} : {}", key, e);
            }
            remainingTimout = remainingTimout - (System.currentTimeMillis() - then);
        }
        return key;
    }

    private CombinedIntervals keyIntervals() {
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

    @SuppressWarnings("unused")
    private void oneRound(ScheduledExecutorService scheduler, Duration duration) {
        CombinedIntervals keyIntervals = keyIntervals();
        List<Digest> have = store.have(keyIntervals);
        gossiper.execute((link, ring) -> link.intervals(keyIntervals.toIntervals(), have),
                          (futureSailor, link, ring) -> {
                              Entries entries;
                              try {
                                  entries = futureSailor.get();
                                  store.add(entries.getRecordsList(), have);
                              } catch (InterruptedException | ExecutionException e) {
                                  log.debug("Error interval gossiping with {} : {}", link.getMember(), e.getCause());
                              }
                              scheduler.schedule(() -> oneRound(scheduler, duration), duration.toMillis(),
                                                 TimeUnit.MILLISECONDS);
                          });

    }
}
