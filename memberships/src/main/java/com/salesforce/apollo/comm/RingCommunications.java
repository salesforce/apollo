/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.Ring.IterateResult;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class RingCommunications<Comm extends Link> {
    public enum Direction {
        PREDECESSOR {
            @Override
            public Member retrieve(Ring<Member> ring, Digest hash, Function<Member, IterateResult> test) {
                return ring.findPredecessor(hash, test);
            }

            @Override
            public Member retrieve(Ring<Member> ring, Member member, Function<Member, IterateResult> test) {
                return ring.findPredecessor(member, test);
            }
        },
        SUCCESSOR {
            @Override
            public Member retrieve(Ring<Member> ring, Digest hash, Function<Member, IterateResult> test) {
                return ring.findSuccessor(hash, test);
            }

            @Override
            public Member retrieve(Ring<Member> ring, Member member, Function<Member, IterateResult> test) {
                return ring.findSuccessor(member, test);
            }
        };

        public abstract Member retrieve(Ring<Member> ring, Digest hash, Function<Member, IterateResult> test);

        public abstract Member retrieve(Ring<Member> ring, Member member, Function<Member, IterateResult> test);
    }

    record linkAndRing<T> (T link, int ring) {}

    private final static Logger log = LoggerFactory.getLogger(RingCommunications.class);

    final Context<Member> context;
    final Executor        exec;
    final SigningMember   member;

    private final CommonCommunications<Comm, ?>  comm;
    private final Direction                      direction;
    private volatile int                         lastRingIndex  = -1;
    private final AtomicReference<List<Integer>> traversalOrder = new AtomicReference<>();

    public RingCommunications(Context<Member> context, SigningMember member, CommonCommunications<Comm, ?> comm,
                              Executor exec) {
        this(Direction.SUCCESSOR, context, member, comm, exec);
    }

    public RingCommunications(Direction direction, Context<Member> context, SigningMember member,
                              CommonCommunications<Comm, ?> comm, Executor exec) {
        assert direction != null && context != null && member != null && comm != null;
        this.direction = direction;
        this.context = context;
        this.member = member;
        this.comm = comm;
        this.exec = exec;
        var order = new ArrayList<Integer>();
        for (int i = 0; i < context.getRingCount(); i++) {
            order.add(i);
        }
        Entropy.secureShuffle(order);
        traversalOrder.set(order);
    }

    public <T> void execute(BiFunction<Comm, Integer, ListenableFuture<T>> round, Handler<T, Comm> handler) {
        final var next = nextRing(null);
        try (Comm link = next.link) {
            execute(round, handler, link, next.ring);
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    public void reset() {
        setLastRingIndex(0);
    }

    @Override
    public String toString() {
        return "RingCommunications [" + context.getId() + ":" + member.getId() + ":" + getLastRing() + "]";
    }

    protected Logger getLog() {
        return log;
    }

    int lastRingIndex() {
        final var c = lastRingIndex;
        return c;
    }

    linkAndRing<Comm> nextRing(Digest digest, Function<Member, IterateResult> test) {
        final int last = lastRingIndex;
        int rings = context.getRingCount();
        int current = (last + 1) % rings;
        if (current == 0) {
            final var order = getTraversalOrder();
            Entropy.secureShuffle(order);
            traversalOrder.set(order);
        }
        lastRingIndex = current;
        return linkFor(digest, current, test);
    }

    linkAndRing<Comm> nextRing(Member member) {
        final int last = lastRingIndex;
        int rings = context.getRingCount();
        int current = (last + 1) % rings;
        if (current == 0) {
            final var order = getTraversalOrder();
            Entropy.secureShuffle(order);
            traversalOrder.set(order);
        }
        lastRingIndex = current;
        return linkFor(current);
    }

    private <T> void execute(BiFunction<Comm, Integer, ListenableFuture<T>> round, Handler<T, Comm> handler, Comm link,
                             int ring) {
        if (link == null) {
            handler.handle(Optional.empty(), link, ring);
        } else {
            ListenableFuture<T> futureSailor = round.apply(link, ring);
            if (futureSailor == null) {
                handler.handle(Optional.empty(), link, ring);
            } else {
                try {
                    futureSailor.addListener(Utils.wrapped(() -> {
                        handler.handle(Optional.of(futureSailor), link, ring);
                    }, log), exec);
                } catch (RejectedExecutionException e) {
                    // ignore
                }
            }
        }
    }

    private int getLastRing() {
        final int current = lastRingIndex;
        return getTraversalOrder().get(current);
    }

    private linkAndRing<Comm> linkFor(Digest digest, int index, Function<Member, IterateResult> test) {
        int r = getTraversalOrder().get(index);
        Ring<Member> ring = context.ring(r);
        Member successor = direction.retrieve(ring, digest, test);
        if (successor == null) {
            return new linkAndRing<>(null, r);
        }
        try {
            return new linkAndRing<>(comm.apply(successor, member), r);
        } catch (Throwable e) {
            log.trace("error opening connection to {}: {} on: {}", successor.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage(), member.getId());
            return new linkAndRing<>(null, r);
        }
    }

    private linkAndRing<Comm> linkFor(int index) {
        int r = getTraversalOrder().get(index);
        Ring<Member> ring = context.ring(r);
        Member successor = direction.retrieve(ring, member, m -> {
            if (!context.isActive(m)) {
                return IterateResult.CONTINUE;
            }
            return IterateResult.SUCCESS;
        });
        if (successor == null) {
            return new linkAndRing<>(null, r);
        }
        try {
            final var link = comm.apply(successor, member);
            log.debug("Using member: {} on ring: {} members: {} on: {}",
                      link == null ? null : link.getMember() == null ? null : link.getMember().getId(), r, ring.size(),
                      member.getId());
            return new linkAndRing<>(link, r);
        } catch (Throwable e) {
            if (log.isDebugEnabled()) {
                log.error("error opening connection to {}: {} on: {}", successor.getId(), member.getId(), e);
            } else {
                log.error("error opening connection to {}: {} on: {}", successor.getId(), member.getId(),
                          (e.getCause() != null ? e.getCause() : e).getMessage(), e);
            }
            return new linkAndRing<>(null, r);
        }
    }

    private void setLastRingIndex(int lastRing) {
        this.lastRingIndex = lastRing;
    }

    private List<Integer> getTraversalOrder() {
        return traversalOrder.get();
    }
}
