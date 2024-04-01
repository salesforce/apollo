/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ring;

import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.context.Context;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Entropy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author hal.hildebrand
 */
public class RingCommunications<T extends Member, Comm extends Link> {
    private final static Logger log = LoggerFactory.getLogger(RingCommunications.class);

    final         Context<T>                    context;
    final         SigningMember                 member;
    private final CommonCommunications<Comm, ?> comm;
    private final Direction                     direction;
    private final boolean                       ignoreSelf;
    private final Lock                          lock           = new ReentrantLock();
    private final List<iteration<T>>            traversalOrder = new ArrayList<>();
    protected     boolean                       noDuplicates   = true;
    volatile      int                           currentIndex   = -1;

    public RingCommunications(Context<T> context, SigningMember member, CommonCommunications<Comm, ?> comm) {
        this(context, member, comm, false);
    }

    public RingCommunications(Context<T> context, SigningMember member, CommonCommunications<Comm, ?> comm,
                              boolean ignoreSelf) {
        this(Direction.SUCCESSOR, context, member, comm, ignoreSelf);
    }

    public RingCommunications(Direction direction, Context<T> context, SigningMember member,
                              CommonCommunications<Comm, ?> comm, boolean ignoreSelf) {
        assert direction != null && context != null && member != null && comm != null;
        this.direction = direction;
        this.context = context;
        this.member = member;
        this.comm = comm;
        this.ignoreSelf = ignoreSelf;
    }

    public RingCommunications<T, Comm> allowDuplicates() {
        noDuplicates = false;
        return this;
    }

    public <Q> void execute(BiFunction<Comm, Integer, Q> round, SyncHandler<T, Q, Comm> handler) {
        final var next = next(member.getId());
        if (next == null || next.member == null) {
            log.debug("No member for ring: {} on: {}", next == null ? "<unkown>" : next.ring, member.getId());
            handler.handle(Optional.empty(), next);
            return;
        }
        try (Comm link = next.link) {
            log.debug("Executing ring: {} to: {} on: {}", next.ring, next.member.getId(), member.getId());
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
        currentIndex = 0;
        traversalOrder.clear();
        log.trace("Reset on: {}", member.getId());
    }

    @Override
    public String toString() {
        return "RingCommunications [" + context.getId() + ":" + member.getId() + ":" + currentIndex + "]";
    }

    @SuppressWarnings("unchecked")
    List<iteration<T>> calculateTraversal(Digest digest) {
        var traversal = new ArrayList<iteration<T>>();
        var traversed = new TreeSet<T>();
        for (int ring = 0; ring < context.getRingCount(); ring++) {
            T successor = direction.retrieve(context, ring, digest, m -> {
                if (ignoreSelf && m.equals(member)) {
                    return Context.IterateResult.CONTINUE;
                }
                if (noDuplicates) {
                    if (traversed.add(m)) {
                        return Context.IterateResult.SUCCESS;
                    } else {
                        return Context.IterateResult.CONTINUE;
                    }
                }
                return Context.IterateResult.SUCCESS;
            });
            if (successor != null) {
                traversal.add(new iteration<>(successor == null ? (T) member : successor, ring));
            }
        }
        return traversal;
    }

    final RingCommunications.Destination<T, Comm> next(Digest digest) {
        lock.lock();
        try {
            final var current = currentIndex;
            final var count = traversalOrder.size();
            if (count == 0 || current == count - 1) {
                traversalOrder.clear();
                traversalOrder.addAll(calculateTraversal(digest));
                Entropy.secureShuffle(traversalOrder);
                log.trace("New traversal order: {}:{} on: {}", context.getRingCount(), traversalOrder, member.getId());
            }
            int next = count == 0 ? 0 : (current + 1) % count;
            currentIndex = next;
            return linkFor(digest);
        } finally {
            lock.unlock();
        }
    }

    protected Logger getLog() {
        return log;
    }

    private <Q> void execute(BiFunction<Comm, Integer, Q> round, SyncHandler<T, Q, Comm> handler,
                             Destination<T, Comm> destination) {
        if (destination.link == null) {
            handler.handle(Optional.empty(), destination);
        } else {
            Q result = null;
            try {
                result = round.apply(destination.link, destination.ring);
            } catch (Throwable e) {
                log.trace("error applying round to: {} on: {}", destination.member.getId(), member.getId(), e);
            }
            handler.handle(Optional.ofNullable(result), destination);
        }
    }

    private Destination<T, Comm> linkFor(Digest digest) {
        if (traversalOrder.isEmpty()) {
            log.trace("No members to traverse on: {}", member.getId());
            return null;
        }
        final var current = currentIndex;
        iteration<T> successor = null;
        try {
            successor = traversalOrder.get(current);
            final Comm link = comm.connect(successor.m);
            if (link == null) {
                log.trace("No connection to {} on: {}", successor.m == null ? "<null>" : successor.m.getId(),
                          member.getId());
            }
            return new Destination<>(successor.m, link, successor.ring);
        } catch (Throwable e) {
            log.trace("error opening connection to {}: {} on: {}", successor.m == null ? "<null>" : successor.m.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage(), member.getId());
            return new Destination<>(successor.m, null, successor.ring);
        }
    }

    public enum Direction {
        PREDECESSOR {
            @Override
            public <T extends Member> T retrieve(Context<T> context, int ring, Digest hash,
                                                 Function<T, Context.IterateResult> test) {
                return context.findPredecessor(ring, hash, test);
            }

            @Override
            public <T extends Member> T retrieve(Context<T> context, int ring, T member,
                                                 Function<T, Context.IterateResult> test) {
                return context.findPredecessor(ring, member, test);
            }
        }, SUCCESSOR {
            @Override
            public <T extends Member> T retrieve(Context<T> context, int ring, Digest hash,
                                                 Function<T, Context.IterateResult> test) {
                return context.findSuccessor(ring, hash, test);
            }

            @Override
            public <T extends Member> T retrieve(Context<T> context, int ring, T member,
                                                 Function<T, Context.IterateResult> test) {
                return context.findSuccessor(ring, member, test);
            }
        };

        public abstract <T extends Member> T retrieve(Context<T> context, int ring, Digest hash,
                                                      Function<T, Context.IterateResult> test);

        public abstract <T extends Member> T retrieve(Context<T> context, int ring, T member,
                                                      Function<T, Context.IterateResult> test);
    }

    public record Destination<M, Q>(M member, Q link, int ring) {
    }

    private record iteration<T extends Member>(T m, int ring) {

        @Override
        public String toString() {
            return String.format("[%s,%s]", m == null ? "<null>" : m.getId(), ring);
        }

    }
}
