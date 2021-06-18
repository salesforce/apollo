/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.comm.Router.CommonCommunications;

/**
 * @author hal.hildebrand
 *
 */
public class Gossiper<Comm extends Link> {

    private final static Logger log = LoggerFactory.getLogger(Gossiper.class);

    private final CommonCommunications<Comm, ?> comm;
    private final Context<Member>               context;
    private final Executor                      executor;
    private volatile int                        lastRing = -1;
    private final SigningMember                 member;

    public Gossiper(Context<Member> context, SigningMember member, CommonCommunications<Comm, ?> comm,
            Executor executor) {
        this.context = context;
        this.executor = executor;
        this.member = member;
        this.comm = comm;
    }

    public <T> void oneRound(BiFunction<Comm, Integer, ListenableFuture<T>> round, Handler<T, Comm> handler) {
        oneRound(member, round, handler);
    }

    public <T> void oneRound(Member key, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                             Handler<T, Comm> handler) {
        try (Comm link = nextRing(key)) {
            if (link == null) {
                handler.handle(null, link, lastRing);
            } else {
                ListenableFuture<T> futureSailor = round.apply(link, lastRing);
                if (futureSailor == null) {
                    handler.handle(null, link, lastRing);
                } else {
                    futureSailor.addListener(() -> handler.handle(futureSailor, link, lastRing), executor);
                }
            }
        } catch (IOException e) {
            log.debug("Error closing");
        }
    }

    public void stop() {
        lastRing = -1;
    }

    @Override
    public String toString() {
        return "Gossiper [" + context.getId() + ":" + member.getId() + ":" + lastRing + "]";
    }

    private Comm linkFor(Member key, Integer ring) {
        Member successor = context.ring(ring).successor(key);
        if (successor == null) {
            log.debug("No successor to node on ring: {} members: {}", ring, context.ring(ring).size());
            return null;
        }
        try {
            return comm.apply(successor, member);
        } catch (Throwable e) {
            log.debug("error opening connection to {}: {}", successor.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private Comm nextRing(Member key) {
        Comm link = null;
        int last = lastRing;
        int rings = context.getRingCount();
        int current = (last + 1) % rings;
        for (int i = 0; i < rings; i++) {
            link = linkFor(key, current);
            if (link != null) {
                break;
            }
            current = (current + 1) % rings;
        }
        lastRing = current;
        return link;
    }
}
