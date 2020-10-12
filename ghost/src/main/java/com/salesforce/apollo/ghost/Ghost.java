/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import static com.salesforce.apollo.ghost.communications.GhostClientCommunications.getCreate;
import static com.salesforce.apollo.protocols.Conversion.hashOf;
import static com.salesforce.apollo.protocols.HashKey.LAST;
import static com.salesforce.apollo.protocols.HashKey.ORIGIN;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesfoce.apollo.proto.DagEntry;
import com.salesfoce.apollo.proto.Interval;
import com.salesforce.apollo.comm.CommonCommunications;
import com.salesforce.apollo.comm.Communications;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.Participant;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.View.MembershipListener;
import com.salesforce.apollo.fireflies.View.MessageChannelHandler;
import com.salesforce.apollo.ghost.communications.GhostClientCommunications;
import com.salesforce.apollo.ghost.communications.GhostServerCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.protocols.HashKey;

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
 * Ghost reuses the t+1 rings of the Fireflies view as the redundant storage
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
 * with its n successors on each of the t+1 rings of the Fireflies View. Because
 * all content is stored redundantly, all lookups for validated, previously
 * stored content will be available whp, assuming non catastrophic loss/gain in
 * membership. The reuse of the t+1 rings of the underlying FF View for storage
 * redundancy sets the upper bounds on the "catastrophic" cardinality and allows
 * Ghost to update the storage for dynamic rebalancing during membership
 * changes. As long as at least 1 of the t+1 members remain as the storage node
 * during the view membership change on a querying node, the DHT will return the
 * result.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Ghost {

    public static class GhostParameters {
        public long     maxConnections = 3;
        public int      redundancy     = 3;
        public long     timeout        = 30;
        public TimeUnit unit           = TimeUnit.SECONDS;
    }

    public class Listener implements MessageChannelHandler, MembershipListener {

        @Override
        public void fail(Participant member) {
            joining.remove(member);
        }

        @Override
        public void message(List<Msg> messages) {
            messages.forEach(msg -> {
                joining.remove(msg.from);
            });
        }

        @Override
        public void recover(Participant member) {
            joining.add(member);
        }

        public void round() {
            if (!joined()) {
                service.join();
                return;
            }
        }

    }

    /**
     * The network service interface
     */
    public class Service {
        private final AtomicBoolean busy    = new AtomicBoolean();
        private final AtomicBoolean started = new AtomicBoolean();

        public DagEntry get(HashKey key) {
            return store.get(key);
        }

        private void join() {
            if (!started.get()) {
                return;
            }

            if (!busy.compareAndSet(false, true)) {
                log.trace("Busy");
            }
            try {
                CombinedIntervals keyIntervals = keyIntervals();
                List<HashKey> have = store.have(keyIntervals);
                int zeros = 0;

                for (int i = 0; i < view.getRings().size(); i++) {
                    Member target = view.getRing(i).successor(getNode(), m -> m.isLive());
                    if (target == null) {
                        log.debug("No target on ring: {}", i);
                        continue;
                    }
                    assert !target.equals(getNode());
                    GhostClientCommunications connection = communications.apply(target, getNode());
                    if (connection == null) {
                        log.debug("No connection for intervals on ring: {} to: {} ", i, target.getId());
                        continue;
                    }
                    try {
                        try {
                            List<DagEntry> entries = connection.intervals(keyIntervals.toIntervals(), have);
                            if (entries.isEmpty()) {
                                zeros++;
                            }
                            store.add(entries, have);
                        } catch (Throwable e) {
                            log.debug("Error interval gossiping with {} : {}", target, e.getCause());
                            continue;
                        }
                    } finally {
                        connection.release();
                    }
                }
                if (zeros == view.getRings().size()) {
                    joined.set(true);
                    view.publish(JOIN_MESSAGE_CHANNEL, "joined".getBytes());
                }
            } finally {
                busy.set(false);
            }
        }

        public List<DagEntry> intervals(List<Interval> intervals, List<HashKey> have) {
            return store.entriesIn(new CombinedIntervals(
                    intervals.stream().map(e -> new KeyInterval(e)).collect(Collectors.toList())), have);
        }

        public Void put(DagEntry value) {
            store.put(new HashKey(hashOf(value)), value);
            return null;
        }

        public List<DagEntry> satisfy(List<HashKey> want) {
            return store.getUpdates(want);
        }

        public void start() {
            if (!started.compareAndSet(false, true)) {
                return;
            }
        }

        public void stop() {
            if (!started.compareAndSet(true, false)) {
                return;
            }
        }
    }

    public static final int     JOIN_MESSAGE_CHANNEL = 3;
    private static final Logger log                  = LoggerFactory.getLogger(Ghost.class);

    private final CommonCommunications<GhostClientCommunications> communications;
    private final AtomicBoolean                                   joined   = new AtomicBoolean(false);
    private final ConcurrentSkipListSet<Member>                   joining  = new ConcurrentSkipListSet<>();
    private final Listener                                        listener = new Listener();
    private final GhostParameters                                 parameters;
    private final int                                             rings;
    private final Service                                         service  = new Service();
    private final Store                                           store;
    private final View                                            view;

    public Ghost(GhostParameters p, Communications c, View v, Store s) {
        parameters = p;
        view = v;
        store = s;

        communications = c.create(getNode(), getCreate(),
                                  new GhostServerCommunications(service, c.getClientIdentityProvider()));
        view.register(listener);
        view.register(JOIN_MESSAGE_CHANNEL, listener);
        view.registerRoundListener(() -> listener.round());

        rings = view.getNode().getParameters().toleranceLevel + 1;
    }

    /**
     * Answer the DagEntry associated with the key
     * 
     * @param key
     * @return the DagEntry associated with ye key
     */
    public DagEntry getDagEntry(HashKey key) {
        if (!joined()) {
            throw new IllegalStateException("Node has not joined the cluster");
        }
        CompletionService<DagEntry> frist = new ExecutorCompletionService<>(ForkJoinPool.commonPool());
        List<Future<DagEntry>> futures;
        futures = IntStream.range(0, rings).mapToObj(i -> view.getRing(i)).map(ring -> frist.submit(() -> {
            Member successor = ring.successor(key, m -> m.isLive() && !joining.contains(m));
            if (successor != null) {
                DagEntry DagEntry;
                GhostClientCommunications connection = communications.apply(successor, getNode());
                if (connection == null) {
                    log.debug("Error looking up {} on {} connection is null", key, successor);
                    return null;
                }
                try {
                    DagEntry = connection.get(key);
                } catch (Exception e) {
                    log.debug("Error looking up {} on {} : {}", key, successor, e);
                    return null;
                } finally {
                    connection.release();
                }
                log.debug("ring {} on {} get {} from {} get: {}", ring.getIndex(), getNode().getId(), key,
                          successor.getId(), DagEntry != null);
                return DagEntry;
            }
            return null;
        })).collect(Collectors.toList());

        int retries = futures.size();
        long remainingTimout = parameters.unit.toMillis(parameters.timeout);

        try {
            DagEntry DagEntry = null;
            while (DagEntry == null && --retries >= 0) {
                long then = System.currentTimeMillis();
                Future<DagEntry> result = null;
                try {
                    result = frist.poll(remainingTimout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.debug("interrupted retrieving key: {}", key);
                    return null;
                }
                try {
                    DagEntry = result.get();
                } catch (InterruptedException e) {
                    return null;
                } catch (ExecutionException e) {
                    log.debug("exception retrieving key: {}: {}", key, e);
                }
                remainingTimout = remainingTimout - (System.currentTimeMillis() - then);
            }
            return DagEntry;
        } finally {
            futures.forEach(f -> f.cancel(true));
        }
    }

    /**
     * @return the Fireflies Node this Ghost instance is leveraging
     */
    public Node getNode() {
        return view.getNode();
    }

    /**
     * @return the network service singleton
     */
    public Service getService() {
        return service;
    }

    /**
     * 
     * @return true if the node has joined the cluster, false otherwise
     */
    public boolean joined() {
        return joined.get();
    }

    /**
     * Insert the DagEntry into the Ghost DHT. Return when a majority of rings have
     * stored the DagEntry
     * 
     * @param DagEntry
     * @return - the HashKey of the DagEntry
     */
    public HashKey putDagEntry(DagEntry DagEntry) {
        if (!joined()) {
            throw new IllegalStateException("Node has not joined the cluster");
        }
        byte[] digest = hashOf(DagEntry);
        HashKey key = new HashKey(digest);
        CompletionService<Boolean> frist = new ExecutorCompletionService<>(ForkJoinPool.commonPool());
        List<Future<Boolean>> futures = IntStream.range(0, rings)
                                                 .mapToObj(i -> view.getRing(i))
                                                 .map(ring -> frist.submit(() -> {
                                                     Member successor = ring.successor(key);
                                                     if (successor != null) {
                                                         GhostClientCommunications connection = communications.apply(successor,
                                                                                                                     getNode());
                                                         if (connection != null) {
                                                             try {
                                                                 connection.put(DagEntry);
                                                                 log.debug("put {} on {}", key, successor.getId());
                                                                 return true;
                                                             } finally {
                                                                 connection.release();
                                                             }
                                                         } else {
                                                             log.debug("Error inserting {} on {} no connection", key,
                                                                       successor);
                                                         }
                                                     }

                                                     return false;
                                                 }))
                                                 .collect(Collectors.toList());
        int retries = futures.size();
        long remainingTimout = parameters.unit.toMillis(parameters.timeout);

        int remainingAcks = rings;

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
        for (int i = 0; i < rings; i++) {
            Ring<Participant> ring = view.getRing(i);
            Participant predecessor = ring.predecessor(getNode(), n -> n.isLive());
            if (predecessor == null) {
                continue;
            }
            HashKey begin = ring.hash(predecessor);
            HashKey end = ring.hash(getNode());
            if (begin.compareTo(end) > 0) { // wrap around the origin of the ring
                intervals.add(new KeyInterval(end, LAST));
                intervals.add(new KeyInterval(ORIGIN, begin));
            } else {
                intervals.add(new KeyInterval(begin, end));
            }
        }
        return new CombinedIntervals(intervals);
    }
}
