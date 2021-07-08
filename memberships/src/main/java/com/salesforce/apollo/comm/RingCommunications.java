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

    final CommonCommunications<Comm, ?> comm;
    final Context<Member>               context;
    final Direction                     direction;
    final Executor                      executor;
    volatile int                        lastRingIndex = -1;
    final SigningMember                 member;
    final List<Integer>                 traversalOrder;

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
        try (Comm link = nextRing(null)) {
            executor.execute(Utils.wrapped(() -> execute(round, handler, link), log));
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    public <T> void execute(Digest digest, BiFunction<Comm, Integer, ListenableFuture<T>> round,
                            Handler<T, Comm> handler) {
        try (Comm link = nextRing(digest)) {
            executor.execute(Utils.wrapped(() -> execute(round, handler, link), log));
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    public void reset() {
        setLastRingIndex(-1);
    }

    @Override
    public String toString() {
        return "RingCommunications [" + context.getId() + ":" + member.getId() + ":" + getLastRing() + "]";
    }

    Comm nextRing(Digest digest) {
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
                futureSailor.addListener(Utils.wrapped(() -> {
                    handler.handle(Optional.of(futureSailor), link, current);
                }, log), executor);
            }
        }
    }

    private int getLastRing() {
        final int current = lastRingIndex;
        if (current < 0) {// shutdown
            return 0; 
        }
        return traversalOrder.get(current);
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

    private void setLastRingIndex(int lastRing) {
        this.lastRingIndex = lastRing;
    }
}
