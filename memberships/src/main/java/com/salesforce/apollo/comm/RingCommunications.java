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
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.Ring;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class RingCommunications<Comm extends Link> {
    public enum Direction {
        PREDECESSOR {
            @Override
            public Member retrieve(Ring<Member> ring, Digest hash, Predicate<Member> test) {
                return ring.predecessor(hash, test);
            }

            @Override
            public Member retrieve(Ring<Member> ring, Member member) {
                return ring.predecessor(member);
            }
        },
        SUCCESSOR {
            @Override
            public Member retrieve(Ring<Member> ring, Digest hash, Predicate<Member> test) {
                return ring.successor(hash, test);
            }

            @Override
            public Member retrieve(Ring<Member> ring, Member member) {
                return ring.successor(member);
            }
        };

        public abstract Member retrieve(Ring<Member> ring, Digest hash, Predicate<Member> test);

        public abstract Member retrieve(Ring<Member> ring, Member member);
    }

    record linkAndRing<T> (T link, int ring) {}

    private final static Logger log = LoggerFactory.getLogger(RingCommunications.class);

    final Context<Member> context;
    final Executor        executor;
    final SigningMember   member;

    private final CommonCommunications<Comm, ?> comm;
    private final Direction                     direction;
    private volatile int                        lastRingIndex = 0;
    private final List<Integer>                 traversalOrder;

    public RingCommunications(Context<Member> context, SigningMember member, CommonCommunications<Comm, ?> comm,
                              Executor executor) {
        this(Direction.SUCCESSOR, context, member, comm, executor);
    }

    public RingCommunications(Direction direction, Context<Member> context, SigningMember member,
                              CommonCommunications<Comm, ?> comm, Executor executor) {
        assert executor != null && direction != null && context != null && member != null && comm != null;
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
        final var next = nextRing(null);
        try (Comm link = next.link) {
            execute(round, handler, link, next.ring);
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    public <T> void execute(Digest digest, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                            Handler<T, Comm> handler, Predicate<Member> test) {
        final var next = nextRing(digest, test);
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

    int lastRingIndex() {
        final var c = lastRingIndex;
        return c;
    }

    linkAndRing<Comm> nextRing(Digest digest, Predicate<Member> test) {
        linkAndRing<Comm> link = null;
        final int last = lastRingIndex;
        int rings = context.getRingCount();
        int current = (last + 1) % rings;
        if (current == 0) {
            Collections.shuffle(traversalOrder);
        }
        for (int i = 0; i < rings; i++) {
            link = linkFor(digest, current, test);
            if (link != null) {
                break;
            }
            current = (current + 1) % rings;
        }
        lastRingIndex = current;
        return link;
    }

    linkAndRing<Comm> nextRing(Member member) {
        linkAndRing<Comm> link = null;
        final int last = lastRingIndex;
        int rings = context.getRingCount();
        int current = (last + 1) % rings;
        if (current == 0) {
            Collections.shuffle(traversalOrder);
        }
        for (int i = 0; i < rings; i++) {
            link = linkFor(current);
            if (link != null) {
                break;
            }
            current = (current + 1) % rings;
        }
        lastRingIndex = current;
        return link;
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
                futureSailor.addListener(Utils.wrapped(() -> {
                    handler.handle(Optional.of(futureSailor), link, ring);
                }, log), executor);
            }
        }
    }

    private int getLastRing() {
        final int current = lastRingIndex;
        return traversalOrder.get(current);
    }

    private linkAndRing<Comm> linkFor(Digest digest, int index, Predicate<Member> test) {
        int r = traversalOrder.get(index);
        Ring<Member> ring = context.ring(r);
        Member successor = direction.retrieve(ring, digest, test);
        if (successor == null) {
            log.debug("No successor to: {} on ring: {} members: {}", digest == null ? member : digest, r, ring.size());
            return null;
        }
        try {
            return new linkAndRing<>(comm.apply(successor, member), r);
        } catch (Throwable e) {
            log.trace("error opening connection to {}: {}", successor.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private linkAndRing<Comm> linkFor(int index) {
        int r = traversalOrder.get(index);
        Ring<Member> ring = context.ring(r);
        Member successor = direction.retrieve(ring, member);
        if (successor == null) {
            log.debug("No successor to: {} on ring: {} members: {}", member, r, ring.size());
            return null;
        }
        try {
            return new linkAndRing<>(comm.apply(successor, member), r);
        } catch (Throwable e) {
            log.trace("error opening connection to {}: {}", successor.getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private void setLastRingIndex(int lastRing) {
        this.lastRingIndex = lastRing;
    }
}
