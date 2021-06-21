/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    public enum Direction {
        PREDECESSOR {
            @Override
            public Member retrieve(Ring<Member> ring, Digest hash, Member member) {
                return hash == null ? ring.predecessor(member) : ring.predecessor(hash);
            }
        },
        SUCCESSOR {
            @Override
            public Member retrieve(Ring<Member> ring, Digest hash, Member member) {
                return hash == null ? ring.successor(member) : ring.successor(hash);
            }
        };

        public abstract Member retrieve(Ring<Member> ring, Digest hash, Member member);
    }

    private final static Logger log = LoggerFactory.getLogger(RingCommunications.class);

    private final CommonCommunications<Comm, ?> comm;
    private final Context<Member>               context;
    private final Direction                     direction;
    private final Executor                      executor;
    private volatile int                        lastRingIndex = -1;
    private final SigningMember                 member;
    private final List<Integer>                 traversalOrder;

    public RingCommunications(Context<Member> context, SigningMember member, CommonCommunications<Comm, ?> comm,
            Executor executor) {
        this(Direction.SUCCESSOR, context, member, comm, executor);
    }

    public RingCommunications(Direction direction, Context<Member> context, SigningMember member,
            CommonCommunications<Comm, ?> comm, Executor executor) {
        this.direction = direction;
        this.context = context;
        this.executor = executor;
        this.member = member;
        this.comm = comm;
        traversalOrder = new ArrayList<>();
        for (int i = 0; i < context.getRingCount(); i++) {
            traversalOrder.add(i);
        }
    }

    public <T> void execute(BiFunction<Comm, Integer, ListenableFuture<T>> round, Handler<T, Comm> handler) {
        try (Comm link = nextRing(null)) {
            execute(round, handler, link);
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    public <T> void execute(Digest digest, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                            Handler<T, Comm> handler) {
        try (Comm link = nextRing(digest)) {
            execute(round, handler, link);
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    public <T> void iterate(Digest digest, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                            PredicateHandler<T, Comm> handler) {
        iterate(digest, null, round, null, handler, null);
    }

    public <T> void iterate(Digest digest, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                            PredicateHandler<T, Comm> handler, Runnable onComplete) {
        iterate(digest, null, round, null, handler, onComplete);
    }

    public <T> void iterate(Digest digest, Runnable onMajority, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                            Runnable failedMajority, PredicateHandler<T, Comm> handler, Runnable onComplete) {
        AtomicInteger tally = new AtomicInteger(0);
        executor.execute(() -> internalIterate(digest, round, handler, onMajority, failedMajority, tally, onComplete));

    }

    public void reset() {
        setLastRingIndex(-1);
    }

    @Override
    public String toString() {
        return "RingCommunications [" + context.getId() + ":" + member.getId() + ":" + getLastRing() + "]";
    }

    <T> void internalIterate(Digest digest, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                             PredicateHandler<T, Comm> handler, Runnable onMajority, Runnable failedMajority,
                             AtomicInteger tally, Runnable onComplete) {
        Runnable proceed = () -> internalIterate(digest, round, handler, onMajority, failedMajority, tally, onComplete);
        try (Comm link = nextRing(digest)) {
            final int current = getLastRing();
            int ringCount = context.getRingCount();
            boolean finalIteration = current % ringCount >= ringCount - 1;
            int majority = context.majority();
            Consumer<Boolean> allowed = allow -> proceed(allow, tally, majority, finalIteration, onMajority,
                                                         failedMajority, proceed, onComplete);
            if (link == null) {
                log.trace("No successor found of: {} on: {} ring: {}  on: {}", digest, context.getId(), current,
                          member);
                allowed.accept(handler.handle(tally, Optional.empty(), link, current));
                return;
            }
            log.trace("Iteration on: {} ring: {} to: {} on: {}", context.getId(), current, link.getMember(), member);
            ListenableFuture<T> futureSailor = round.apply(link, current);
            if (futureSailor == null) {
                log.trace("No asynchronous response for: {} on: {} ring: {} from: {} on: {}", digest, context.getId(),
                          current, link.getMember(), member);
                allowed.accept(handler.handle(tally, Optional.empty(), link, current));
                return;
            }
            futureSailor.addListener(() -> {
                log.trace("Response of: {} on: {} ring: {} from: {} on: {}", digest, context.getId(), current,
                          link.getMember(), member);
                allowed.accept(handler.handle(tally, Optional.of(futureSailor), link, current) && !finalIteration);
            }, executor);
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    void proceed(Boolean allow, AtomicInteger tally, int majority, boolean finalIteration, Runnable onMajority,
                 Runnable failedMajority, Runnable proceed, Runnable onComplete) {
        if (finalIteration) {
            log.trace("Final iteration of: {} tally: {} on: {}", context.getId(), tally.get(), member);
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
            if (onComplete != null) {
                log.trace("Completing iteration for: {} tally: {} on: {}", context.getId(), tally.get(), member);
                onComplete.run();
            }
        } else if (allow) {
            log.trace("Proceeding for: {} tally: {} on: {}", context.getId(), tally.get(), member);
            executor.execute(proceed);
        } else {
            log.trace("Termination of: {} tally: {} on: {}", context.getId(), tally.get(), member);
        }
    }

    private <T> void execute(BiFunction<Comm, Integer, ListenableFuture<T>> round, Handler<T, Comm> handler,
                             Comm link) {
        final int current = getLastRing();
        if (link == null) {
            handler.handle(Optional.empty(), link, current);
        } else {
            ListenableFuture<T> futureSailor = round.apply(link, current);
            if (futureSailor == null) {
                handler.handle(Optional.empty(), link, current);
            } else {
                futureSailor.addListener(() -> {
                    handler.handle(Optional.of(futureSailor), link, current);
                }, executor);
            }
        }
    }

    private int getLastRing() {
        return traversalOrder.get(lastRingIndex);
    }

    private Comm linkFor(Digest digest, int index) {
        int r = traversalOrder.get(index);
        Ring<Member> ring = context.ring(r);
        Member successor = direction.retrieve(ring, digest, member);
        if (successor == null) {
            log.debug("No successor to: {} on ring: {} members: {}", digest, r, ring.size());
            return null;
        }
        try {
            return comm.apply(successor, member);
        } catch (Throwable e) {
            log.trace("error opening connection to {}: {}", successor.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private Comm nextRing(Digest digest) {
        Comm link = null;
        final int last = lastRingIndex;
        int rings = context.getRingCount();
        int current = (last + 1) % rings;
        for (int i = 0; i < rings; i++) {
            if (current == 0) {
                Collections.shuffle(traversalOrder);
            }
            link = linkFor(digest, current);
            if (link != null) {
                break;
            }
            current = (current + 1) % rings;
        }
        lastRingIndex = current;
        return link;
    }

    private void setLastRingIndex(int lastRing) {
        this.lastRingIndex = lastRing;
    }
}
