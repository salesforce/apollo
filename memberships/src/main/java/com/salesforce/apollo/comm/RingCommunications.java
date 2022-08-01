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
import java.util.TreeSet;
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
public class RingCommunications<T extends Member, Comm extends Link> {

    public enum Direction {
        PREDECESSOR {
            @Override
            public <T extends Member> T retrieve(Ring<T> ring, Digest hash, Function<T, IterateResult> test) {
                return ring.findPredecessor(hash, test);
            }

            @Override
            public <T extends Member> T retrieve(Ring<T> ring, T member, Function<T, IterateResult> test) {
                return ring.findPredecessor(member, test);
            }
        },
        SUCCESSOR {
            @Override
            public <T extends Member> T retrieve(Ring<T> ring, Digest hash, Function<T, IterateResult> test) {
                return ring.findSuccessor(hash, test);
            }

            @Override
            public <T extends Member> T retrieve(Ring<T> ring, T member, Function<T, IterateResult> test) {
                return ring.findSuccessor(member, test);
            }
        };

        public abstract <T extends Member> T retrieve(Ring<T> ring, Digest hash, Function<T, IterateResult> test);

        public abstract <T extends Member> T retrieve(Ring<T> ring, T member, Function<T, IterateResult> test);
    }

    public record Destination<M, Q> (M member, Q link, int ring) {}

    private final static Logger log = LoggerFactory.getLogger(RingCommunications.class);

    final Context<T>                             context;
    final Executor                               exec;
    final SigningMember                          member;
    private final CommonCommunications<Comm, ?>  comm;
    private final Direction                      direction;
    private volatile int                         lastRingIndex  = -1;
    private boolean                              noDuplicates   = false;
    private final AtomicReference<List<Integer>> traversalOrder = new AtomicReference<>();

    public RingCommunications(Context<T> context, SigningMember member, CommonCommunications<Comm, ?> comm,
                              Executor exec) {
        this(Direction.SUCCESSOR, context, member, comm, exec);
    }

    public RingCommunications(Direction direction, Context<T> context, SigningMember member,
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

    public <Q> void execute(BiFunction<Comm, Integer, ListenableFuture<Q>> round, Handler<T, Q, Comm> handler) {
        final var next = nextRing(null);
        try (Comm link = next.link) {
            execute(round, handler, next);
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    public RingCommunications<T, Comm> noDuplicates() {
        noDuplicates = true;
        return this;
    }

    public void reset() {
        setLastRingIndex(0);
        var order = new ArrayList<Integer>();
        for (int i = 0; i < context.getRingCount(); i++) {
            order.add(i);
        }
        Entropy.secureShuffle(order);
        traversalOrder.set(order);
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

    Destination<T, Comm> nextRing(Digest digest, Function<T, IterateResult> test) {
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

    Destination<T, Comm> nextRing(T member) {
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

    private <Q> void execute(BiFunction<Comm, Integer, ListenableFuture<Q>> round, Handler<T, Q, Comm> handler,
                             Destination<T, Comm> destination) {
        if (destination.link == null) {
            handler.handle(Optional.empty(), destination);
        } else {
            ListenableFuture<Q> futureSailor = round.apply(destination.link, destination.ring);
            if (futureSailor == null) {
                handler.handle(Optional.empty(), destination);
            } else {
                try {
                    futureSailor.addListener(Utils.wrapped(() -> {
                        handler.handle(Optional.of(futureSailor), destination);
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

    private List<Integer> getTraversalOrder() {
        return traversalOrder.get();
    }

    private Destination<T, Comm> linkFor(Digest digest, int index, Function<T, IterateResult> test) {
        int r = getTraversalOrder().get(index);
        Ring<T> ring = context.ring(r);
        T successor = direction.retrieve(ring, digest, test);
        if (successor == null) {
            return new Destination<>(null, null, r);
        }
        try {
            return new Destination<>(successor, comm.apply(successor, member), r);
        } catch (Throwable e) {
            log.trace("error opening connection to {}: {} on: {}", successor.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage(), member.getId());
            return new Destination<>(successor, null, r);
        }
    }

    private Destination<T, Comm> linkFor(int index) {
        int r = getTraversalOrder().get(index);
        Ring<T> ring = context.ring(r);
        var traversed = new TreeSet<Member>();

        @SuppressWarnings("unchecked")
        T successor = direction.retrieve(ring, (T) member, m -> {
            if (!context.isActive(m)) {
                return IterateResult.CONTINUE;
            }
            if (noDuplicates) {
                if (traversed.add(m)) {
                    return IterateResult.SUCCESS;
                } else {
                    return IterateResult.CONTINUE;
                }
            }
            return IterateResult.SUCCESS;
        });
        if (successor == null) {
            return new Destination<>(null, null, r);
        }
        try {
            final var link = comm.apply(successor, member);
            log.debug("Using member: {} on ring: {} members: {} on: {}", successor.getId(), r, ring.size(),
                      member.getId());
            return new Destination<>(successor, link, r);
        } catch (Throwable e) {
            if (log.isDebugEnabled()) {
                log.error("error opening connection to {}: {} on: {}", successor.getId(), member.getId(), e);
            } else {
                log.error("error opening connection to {}: {} on: {}", successor.getId(), member.getId(),
                          (e.getCause() != null ? e.getCause() : e).getMessage(), e);
            }
            return new Destination<>(successor, null, r);
        }
    }

    private void setLastRingIndex(int lastRing) {
        this.lastRingIndex = lastRing;
    }
}
