/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.web;

import java.time.Duration;
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
import com.salesforce.apollo.web.resources.ByteTransactionApi;
import com.salesforce.apollo.web.resources.DagApi.QueryFinalizedResult;

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
    private final Meter                finalizations;
    private final Meter                submisions;
    private final Timer                latency;

    public Transactioneer(MetricRegistry registry) {
        log = LoggerFactory.getLogger("Transactioneer [" + id);

        submisions = registry.meter("Transaction submit rate");
        finalizations = registry.meter("Transaction finalize rate");
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
                      int maxDelta) {
        final ScheduledFuture<?> queryFuture = query.scheduleWithFixedDelay(queryFinalized(queryEndpoint), 0,
                                                                            queryInterval.toMillis(),
                                                                            TimeUnit.MILLISECONDS);

        final ScheduledFuture<?> submitFuture = submit.scheduleWithFixedDelay(submitTransaction(outstanding,
                                                                                                submitEndpoint,
                                                                                                maxDelta),
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

    private boolean query(String key, WebTarget queryEndpoint) {
        final Response response = queryEndpoint.request().post(Entity.text(key));
        if (!(response.getStatus() == 200)) {
            log.error("Failed querying status of txn #{} response: {}: {}", key, response.getStatus(),
                      response.readEntity(String.class));
            return false;
        }
        if (response.readEntity(QueryFinalizedResult.class).isFinalized()) {
            final Context timer = unfinalized.remove(key);
            timer.stop();
            finalizations.mark();
            finalized.add(key);
            return true;
        }
        return false;
    }

    private Runnable queryFinalized(WebTarget queryEndpoint) {
        return () -> {
            log.info("Querying finalized for {}", id);
            final long txnsFinalized = unfinalized.keySet()
                                                  .parallelStream()
                                                  .map(key -> query(key, queryEndpoint))
                                                  .count();
            if (txnsFinalized > 0) {
                log.info("Finalized: {}", txnsFinalized);
            }
        };
    }

    private Runnable submitTransaction(int outstanding, WebTarget submitEndpoint, int maxDelta) {
        return () -> {
            log.trace("submitting txns for {}", id);
            long then = System.currentTimeMillis();
            int remaining = maxDelta;
            int failed = 0;
            int submitted = 0;
            while (remaining > 0 && unfinalized.size() < outstanding) {
                remaining--;
                if (!submitTxn(submitEndpoint)) {
                    failed++;
                }
                submitted++;
            }
            if (submitted > 0) {
                log.info("Submitted {} txns, {} failed, in {}", submitted, failed, System.currentTimeMillis() - then);
            }
        };
    }

    private Boolean submitTxn(WebTarget submitEndpoint) {
        final Context timer = latency.time();

        Response response = submitEndpoint.request(MediaType.APPLICATION_JSON)
                                          .post(Entity.json(new ByteTransactionApi.ByteTransaction(40_000,
                                                  String.format("Hello World %s from: %s @ %s", id,
                                                                lastTxn.incrementAndGet(), System.currentTimeMillis())
                                                        .getBytes())));

        if (!(response.getStatus() == 200)) {
            log.error("Failed submitting txn #{} response: {}: {}", lastTxn.get(), response.getStatus(),
                      response.readEntity(String.class));
            return false;
        }

        String key = response.readEntity(String.class);
        unfinalized.put(key, timer);
        submisions.mark();

        return true;
    }
}
