/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowman.poll;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.Multiset;
import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
public class VoteSet {
    private static class poll {
        private final Poll    poll;
        private final Context timer;

        public poll(Poll poll, Context timer) {
            this.poll = poll;
            this.timer = timer;
        }
    }

    private final Poll.Factory       factory;
    private final Logger             log;
    private final Timer              pollDurations;
    private final Map<Integer, poll> polls = new ConcurrentHashMap<>();

    public VoteSet(Poll.Factory factory, String namespace, Logger log, MetricRegistry metrics) {
        this.factory = factory;
        this.log = log;
        metrics.register(MetricRegistry.name(namespace, "polls"), new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return polls.size();
            }
        });
        pollDurations = metrics.timer(MetricRegistry.name(namespace, "poll_duration"));

    }

    public boolean add(int requestID, Multiset<ShortID> vdrs) {
        if (polls.containsKey(requestID)) {
            log.debug("dropping poll due to duplicated requestID: {}", requestID);
            return false;
        }

        log.info("creating poll with requestID {} and validators {}", requestID, vdrs);

        polls.put(requestID, new poll(factory.newPoll(vdrs), // create the new poll
                pollDurations.time()));
        return true;
    }

    public Bag Drop(int requestID, ShortID vdr) {
        poll p = polls.get(requestID);
        if (p == null) {
            log.info("dropping vote from {} to an unknown poll with requestID: {}", vdr, requestID);
            return null;
        }

        log.info("processing dropped vote from {} in the poll with requestID: {}", vdr, requestID);

        p.poll.drop(vdr);

        if (!p.poll.finished()) {
            return null;
        }

        log.info("poll with requestID {} finished as {}", requestID, p.poll);

        polls.remove(requestID); // remove the poll from the current set
        p.timer.close();
        return p.poll.result();
    }

    public int size() {
        return polls.size();
    }

    public Bag vote(int requestID, ShortID vdr, ID vote) {
        poll p = polls.get(requestID);
        if (p == null) {
            log.info("dropping vote from {} to an unknown poll with requestID: {}", vdr, requestID);
            return null;
        }

        log.info("processing vote from %s in the poll with requestID: {} with the vote {}", vdr, requestID, vote);

        p.poll.vote(vdr, vote);
        if (!p.poll.finished()) {
            return null;
        }

        log.info("poll with requestID %d finished as %s", requestID, p.poll);

        polls.remove(requestID); // remove the poll from the current set
        p.timer.close();
        return p.poll.result();
    }
}
