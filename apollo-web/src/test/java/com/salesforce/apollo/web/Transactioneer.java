/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.web;

import java.time.Duration;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.salesforce.apollo.web.resources.ByteTransactionApi.ByteTransaction;

/**
 * @author hhildebrand
 *
 */
public class Transactioneer {

    private final Deque<String>        finalized   = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean        finished    = new AtomicBoolean(false);
    private final UUID                 id          = UUID.randomUUID();
    private final AtomicInteger        lastTxn     = new AtomicInteger();
    private final Logger               log;
    private final Map<String, Context> unfinalized = new ConcurrentHashMap<>(); 
    private final Meter                submisions;
    private final Timer                latency;

    public Transactioneer(MetricRegistry registry) {
        log = LoggerFactory.getLogger("Transactioneer [" + id);

        submisions = registry.meter("Transaction submit rate"); 
        latency = registry.timer("Transaction finalization latency");
    }

    public List<String> getFinalized() {
        return finalized.stream().collect(Collectors.toList());
    }

    public UUID getId() {
        return id;
    }

    public AtomicInteger getLastTxn() {
        return lastTxn;
    }

    public List<String> getUnfinalized() {
        return unfinalized.keySet().stream().collect(Collectors.toList());
    }

    public boolean isFinished() {
        return finished.get();
    }

    public void start(ScheduledExecutorService run, Duration duration, ScheduledExecutorService query,
                      Duration queryInterval, WebTarget queryEndpoint, ScheduledExecutorService submit,
                      Duration submitInterval, WebTarget submitEndpoint, int outstanding, Duration initialDelay,
                      int batchSize) {
        final ScheduledFuture<?> queryFuture = query.scheduleWithFixedDelay(queryFinalized(queryEndpoint), 0,
                                                                            queryInterval.toMillis(),
                                                                            TimeUnit.MILLISECONDS);

        final ScheduledFuture<?> submitFuture = submit.scheduleWithFixedDelay(submitTransactions(outstanding,
                                                                                                 submitEndpoint,
                                                                                                 batchSize),
                                                                              initialDelay.toMillis(),
                                                                              submitInterval.toMillis(),
                                                                              TimeUnit.MILLISECONDS);

        run.schedule(() -> {
            finished.set(false);
            queryFuture.cancel(true);
            submitFuture.cancel(true);
            finished.set(true);
        }, duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private Boolean[] query(String[] txns, WebTarget queryEndpoint) {
        final Response response = queryEndpoint.request(MediaType.APPLICATION_JSON).post(Entity.json(txns));
        if (!(response.getStatus() == 200)) {
            log.error("Failed querying status of txns response: {}: {}", response.getStatus(),
                      response.readEntity(String.class));
            Boolean[] results = new Boolean[txns.length];
            Arrays.fill(results, Boolean.FALSE);
            return results;
        }
        final Boolean[] results = response.readEntity(Boolean[].class);
        for (int i = 0; i < results.length; i++) {
            if (results[i]) {
                String txn = txns[i];
                final Context timer = unfinalized.remove(txn);
                timer.stop(); 
                finalized.add(txn);
            }
        }
        return results;
    }

    private Runnable queryFinalized(WebTarget queryEndpoint) {
        return () -> {
            log.trace("Querying finalized for {}", id);
            final List<String> toQuery = unfinalized.keySet().stream().collect(Collectors.toList());
            final Boolean[] txnsFinalized = query(toQuery.toArray(new String[toQuery.size()]), queryEndpoint);
            int finalized = 0;
            for (Boolean r : txnsFinalized) {
                if (r) {
                    finalized++;
                }
            }
            if (finalized > 0) {
                log.debug("Finalized: {}", finalized);
            }
        };
    }

    private Runnable submitTransactions(int outstanding, WebTarget submitEndpoint, int batchSize) {
        return () -> {
            log.trace("submitting txns for {}", id);
            if (unfinalized.size() >= outstanding) {
                return;
            }

            ByteTransaction[] batch = new ByteTransaction[Math.min(batchSize, outstanding - unfinalized.size())];
            for (int i = 0; i < batch.length; i++) {
                batch[i] = new ByteTransaction(-1, String
                                                         .format("Hello World %s from: %s @ %s", id,
                                                                 lastTxn.incrementAndGet(), System.currentTimeMillis())
                                                         .getBytes());

            }
            Response response = submitEndpoint.request(MediaType.APPLICATION_JSON).post(Entity.json(batch));

            if (!(response.getStatus() == 200)) {
                log.error("Failed submitting txn #{} response: {}: {}", lastTxn.get(), response.getStatus(),
                          response.readEntity(String.class));
                return;
            }
            String[] keys = response.readEntity(String[].class);
            submisions.mark(keys.length);
            for (String key : keys) {
                unfinalized.put(key, latency.time());
            }
        };
    }
}
