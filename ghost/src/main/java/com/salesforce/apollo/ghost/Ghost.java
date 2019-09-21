/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ghost;

import static com.salesforce.apollo.protocols.Conversion.hashOf;
import static com.salesforce.apollo.protocols.Conversion.manifestDag;
import static com.salesforce.apollo.protocols.Conversion.serialize;
import static com.salesforce.apollo.protocols.HashKey.LAST;
import static com.salesforce.apollo.protocols.HashKey.ORIGIN;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.avro.AvroRemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.EntryType;
import com.salesforce.apollo.avro.GhostUpdate;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.avro.Interval;
import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.Node;
import com.salesforce.apollo.fireflies.Ring;
import com.salesforce.apollo.fireflies.View;
import com.salesforce.apollo.fireflies.View.ViewListener;
import com.salesforce.apollo.ghost.communications.GhostClientCommunications;
import com.salesforce.apollo.ghost.communications.GhostCommunications;
import com.salesforce.apollo.protocols.HashKey;

/**
 * Spaaaaaaaaaaaace Ghooooooooossssssstttttt.
 * <p>
 * A distributed, content addresssable hash table. Keys of this DHT are the hash of the content's bytes.
 * <p>
 * Builds on the Fireflies membership gossip service (and swiss army knife) to implement a one hop imutable DHT. Stored
 * content is only addressible by the hash of the content. Thus, content is immutable (although we allow deletes,
 * because GC).
 * <p>
 * Ghost reuses the 2t+1 rings of the Fireflies view as the redundant storage rings for content. The hash keys of the
 * content map to each ring differently, and so each Ghost instance stores 2t+1 intervals - perhaps overlapping - of the
 * current content set of the system wide DHT.
 * <p>
 * Content is stored redundantly on 2t+1 rings and Ghost emits n (where n <= 2t+1) parallel communications for key
 * lookup. If the key is stored, the first responder with verified (hash(content) == lookup key) of the parallel query
 * is returned. If the key is not present, the client only waits for the indicated timeout, rather than the sum of
 * timeouts from t+1 serial queries.
 * <p>
 * Content storage operations must complete a majority of writes out of 2t+1 rings to return without error. As the key
 * of any content is its hash, content is immutable, so any put() operation may be retried, as put() is idempotent. Note
 * that idempotent push() does not mean zero overhead for redundant pushes. There still will be communication overhead
 * of at least the majority of ghost nodes on the various rings.
 * <p>
 * To compensate for the wild, wild west of dynamic membership, Ghost gossips with its n successors on each of the 2t+1
 * rings of the Fireflies View. Because all content is stored redundantly, all lookups for validated, previously stored
 * content will be available whp, assuming non catastrophic loss/gain in membership. The reuse of the 2t+1 rings of the
 * underlying FF View for storage redundancy sets the upper bounds on the "catastrophic" cardinality and allows Ghost to
 * update the storage for dynamic rebalancing during membership changes. As long as at least 1 of the 2t+1 members
 * remain as the storage node during the view membership change on a querying node, the DHT will return the result.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Ghost {

    public static class GhostParameters {
        public long maxConnections = 3;
        public int redundancy = 3;
        public long timeout = 30;
        public TimeUnit unit = TimeUnit.SECONDS;
    }

    public class Listener implements ViewListener {

        @Override
        public void fail(Member member) {}

        @Override
        public void message(List<Msg> messages) {
            // TODO Auto-generated method stub

        }

        @Override
        public void recover(Member member) {}

        @Override
        public void round() {
            service.gossip();
        }

    }

    /**
     * The network service interface
     */
    public class Service {
        private volatile int lastRing = -1;
        private final AtomicBoolean started = new AtomicBoolean();

        public Entry get(HASH key) {
            return store.get(key);
        }

        /**
         * 3 phase gossip with the N successors of the next ring
         */
        public void gossip() {
            if (!started.get()) { return; }

            int current = (lastRing + 1) % getNode().getParameters().rings;
            lastRing = current;

            for (int i = 0; i < parameters.redundancy; i++) {
                AtomicInteger span = new AtomicInteger(i);

                Member target = view.getRing(current)
                                    .successor(getNode(), n -> n.isLive() && span.getAndDecrement() == 0);
                if (target == null) {
                    log.trace("Finished {} rounds of gossip on ring {}", parameters.redundancy, current);
                    return;
                }
                log.trace("Round {} of gossip on ring {}", i, current);
                GhostClientCommunications connection = communications.connect(target, getNode());
                if (connection == null) {
                    break;
                }
                GhostUpdate update;
                CombinedIntervals intervals = keyIntervals();
                try {
                    try {
                        update = connection.ghostGossip(intervals.toIntervals(), store.keySet());
                    } catch (AvroRemoteException e) {
                        log.debug("Error gossiping with {} : {}", target, e.getCause());
                        return;
                    }
                    List<Entry> updates = process(update, intervals);
                    if (!updates.isEmpty()) {
                        log.debug("Updates for {} are {}", target.getId(), updates);
                        connection.gUpdate(updates);
                    }
                } finally {
                    connection.close();
                }
            }
        }

        public GhostUpdate gossip(List<Interval> i, List<HASH> digests) {
            return store.updatesFor(new CombinedIntervals(i.stream()
                                                           .map(e -> new KeyInterval(e))
                                                           .collect(Collectors.toList())),
                                    digests.stream().map(e -> new HashKey(e)).collect(Collectors.toList()),
                                    keyIntervals());
        }

        /**
         * @param certificate
         * @return the Member in the View that corresponds to the supplied certificate
         */
        public Member memberFor(X509Certificate certificate) {
            return view.getView().get(Member.getMemberId(certificate));
        }

        public Void put(Entry value) {
            store.put(new HASH(hashOf(value)), value);
            return null;
        }

        public void start(Duration d) {
            if (!started.compareAndSet(false, true)) { return; }
            communications.start();
        }

        public void stop() {
            if (!started.compareAndSet(true, false)) { return; }
            communications.close();
        }

        public Void update(List<Entry> updates) {
            CombinedIntervals intervals = keyIntervals();
            for (Entry entry : updates) {
                HASH hash = new HASH(hashOf(entry));
                if (intervals.test(hash)) {
                    store.put(hash, entry);
                }
            }
            return null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Ghost.class);

    private final GhostCommunications communications;
    private final Listener listener = new Listener();
    private final GhostParameters parameters;
    private final Service service = new Service();
    private final Store store;
    private final View view;

    public Ghost(GhostParameters p, GhostCommunications c, View v, Store s) {
        parameters = p;
        communications = c;
        view = v;
        store = s;
        communications.initialize(this);
        view.register(listener);
    }

    /**
     * @param key
     * @return the DagEntry matching the key
     */
    public DagEntry getDagEntry(HashKey key) {
        Entry entry = getEntry(key);
        if (entry == null) { return null; }
        if (entry.getType() != EntryType.DAG) { throw new IllegalStateException("Not a DAG: " + entry.getType()); }
        return manifestDag(entry);
    }

    /**
     * Answer the Entry associated with the key
     * 
     * @param key
     * @return the Entry associated with ye key
     */
    public Entry getEntry(HashKey key) {
        HASH keyBits = key.toHash();
        CompletionService<Entry> frist = new ExecutorCompletionService<>(ForkJoinPool.commonPool());
        List<Future<Entry>> futures;
        futures = view.getRings().stream().map(ring -> frist.submit(() -> {
            Member successor = ring.successor(key, m -> m.isLive());
            if (successor != null) {
                Entry entry;
                GhostClientCommunications connection = communications.connect(successor, getNode());
                if (connection == null) { return null; }
                try {
                    entry = connection.get(keyBits);
                } catch (AvroRemoteException e) {
                    log.debug("Error looking up {} on {} : {}", key, successor, e);
                    return null;
                } finally {
                    connection.close();
                }
                log.debug("ring {} on {} get {} from {} : {}", ring.getIndex(), getNode().getId(), key,
                          successor.getId(), entry != null);
                return entry;
            }
            return null;
        })).collect(Collectors.toList());

        int retries = futures.size();
        long remainingTimout = parameters.unit.toMillis(parameters.timeout);

        try {
            Entry entry = null;
            while (entry == null && --retries >= 0) {
                long then = System.currentTimeMillis();
                Future<Entry> result = null;
                try {
                    result = frist.poll(remainingTimout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.debug("interrupted retrieving key: {}", key);
                    return null;
                }
                try {
                    entry = result.get();
                } catch (InterruptedException e) {
                    return null;
                } catch (ExecutionException e) {
                    log.debug("exception retrieving key: {}: {}", key, e);
                }
                remainingTimout = remainingTimout - (System.currentTimeMillis() - then);
            }
            return entry;
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
     * Insert the dag entry into the Ghost DHT
     * 
     * @param dag
     * @return the HashKey for the entry
     */
    public HashKey putDagEntry(DagEntry dag) {
        return putEntry(serialize(dag));
    }

    /**
     * Insert the entry into the Ghost DHT
     * 
     * @param entry
     * @return - the HashKey of the entry
     */
    public HashKey putEntry(Entry entry) {
        byte[] digest = hashOf(entry);
        HashKey key = new HashKey(digest);
        view.getRings().stream().forEach(ring -> {
            Member successor = ring.successor(key);
            if (successor != null) {
                GhostClientCommunications connection = communications.connect(successor, getNode());
                if (connection != null) {
                    try {
                        connection.put(entry);
                        log.debug("put {} on {}", key, successor.getId());
                    } finally {
                        connection.close();
                    }

                }
            }
        });
        return key;
    }

    /**
     * Process the updates, storing only the entries that are valid for the Ghost's node
     * 
     * @param update
     * @param intervals
     *            - the key intervals that the receiver is responsible for
     * @return
     */
    List<Entry> process(GhostUpdate update, CombinedIntervals intervals) {
        for (Entry entry : update.getUpdates()) {
            HASH hash = new HASH(hashOf(entry));
            if (intervals.test(hash)) {
                store.put(hash, entry);
            }
        }
        return update.getWant().stream().map(key -> store.get(key)).filter(e -> e != null).collect(Collectors.toList());
    }

    private CombinedIntervals keyIntervals() {
        List<KeyInterval> intervals = new ArrayList<>();
        for (int i = 0; i < getNode().getParameters().rings; i++) {
            Ring ring = view.getRing(i);
            Member predecessor = ring.predecessor(getNode(), n -> n.isLive());
            if (predecessor == null) {
                continue;
            }
            HashKey begin = predecessor.hashFor(ring.getIndex());
            HashKey end = getNode().hashFor(ring.getIndex());
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
