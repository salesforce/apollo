/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 *
 */
public class RingCommunications<Comm extends Link> {

    private final static Logger log = LoggerFactory.getLogger(RingCommunications.class);

    private final CommonCommunications<Comm, ?> comm;
    private final Context<Member>               context;
    private final Executor                      executor;
    private volatile int                        lastRing = -1;
    private final SigningMember                 member;

    public RingCommunications(Context<Member> context, SigningMember member, CommonCommunications<Comm, ?> comm,
            Executor executor) {
        this.context = context;
        this.executor = executor;
        this.member = member;
        this.comm = comm;
    }

    public <T> void execute(BiFunction<Comm, Integer, ListenableFuture<T>> round, Handler<T, Comm> handler) {
        try (Comm link = nextRing(null)) {
            execute(round, handler, link);
        } catch (IOException e) {
            log.debug("Error closing");
        }
    }

    public <T> void execute(Digest digest, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                            Handler<T, Comm> handler) {
        try (Comm link = nextRing(digest)) {
            execute(round, handler, link);
        } catch (IOException e) {
            log.debug("Error closing");
        }
    }

    public <T> void iterate(Digest digest, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                            PredicateHandler<T, Comm> handler) {
        iterate(digest, round, handler, null, null);
    }

    public <T> void iterate(Digest digest, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                            PredicateHandler<T, Comm> handler, Runnable onMajority, Runnable failedMajority) {
        AtomicInteger tally = new AtomicInteger(0);
        Runnable proceed = () -> iterate(digest, round, handler, onMajority, failedMajority);
        try (Comm link = nextRing(digest)) {
            int ringCount = context.getRingCount();
            boolean finalIteration = lastRing % ringCount >= ringCount - 1;
            int majority = context.majority();
            Consumer<Boolean> allowed = allow -> {
                proceed(allow, tally, majority, finalIteration, onMajority, failedMajority, proceed);
            };
            if (link == null) {
                allowed.accept(handler.handle(tally, Optional.empty(), link, lastRing));
                return;
            }
            ListenableFuture<T> futureSailor = round.apply(link, lastRing);
            if (futureSailor == null) {
                allowed.accept(handler.handle(tally, Optional.empty(), link, lastRing));
                return;
            }
            futureSailor.addListener(() -> {
                allowed.accept(handler.handle(tally, Optional.of(futureSailor), link, lastRing) && !finalIteration);
            }, executor);
        } catch (IOException e) {
            log.debug("Error closing");
        }

    }

    void proceed(Boolean allow, AtomicInteger tally, int majority, boolean finalIteration, Runnable onMajority,
                 Runnable failedMajority, Runnable proceed) {
        if (finalIteration) {
            if (failedMajority != null) {
                if (tally.get() < majority) {
                    failedMajority.run();
                }
            }
            if (onMajority != null) {
                if (tally.get() >= majority) {
                    onMajority.run();
                }
            }
        } else if (allow) {
            proceed.run();
        }
    }

    public void reset() {
        lastRing = -1;
    }

    @Override
    public String toString() {
        return "RingCommunications [" + context.getId() + ":" + member.getId() + ":" + lastRing + "]";
    }

    private <T> void execute(BiFunction<Comm, Integer, ListenableFuture<T>> round, Handler<T, Comm> handler,
                             Comm link) {
        if (link == null) {
            handler.handle(Optional.empty(), link, lastRing);
        } else {
            ListenableFuture<T> futureSailor = round.apply(link, lastRing);
            if (futureSailor == null) {
                handler.handle(Optional.empty(), link, lastRing);
            } else {
                futureSailor.addListener(() -> {
                    handler.handle(Optional.of(futureSailor), link, lastRing);
                }, executor);
            }
        }
    }

    private Comm linkFor(Digest digest, int r) {
        Ring<Member> ring = context.ring(r);
        Member successor = digest == null ? ring.successor(member) : ring.successor(digest);
        if (successor == null) {
            log.debug("No successor to: {} on ring: {} members: {}", digest, r, ring.size());
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

    private Comm nextRing(Digest digest) {
        Comm link = null;
        int last = lastRing;
        int rings = context.getRingCount();
        int current = (last + 1) % rings;
        for (int i = 0; i < rings; i++) {
            link = linkFor(digest, current);
            if (link != null) {
                break;
            }
            current = (current + 1) % rings;
        }
        lastRing = current;
        return link;
    }
}
