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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

/**
 * @author hal.hildebrand
 */
public class RingCommunications<T extends Member, Comm extends Link> {
    private final static Logger log = LoggerFactory.getLogger(RingCommunications.class);

    final         Context<T>                    context;
    final         SigningMember                 member;
    private final CommonCommunications<Comm, ?> comm;
    private final Lock                          lock           = new ReentrantLock();
    private final List<Context.iteration<T>>    traversalOrder = new ArrayList<>();
    protected     boolean                       noDuplicates   = true;
    volatile      int                           currentIndex   = -1;
    private       boolean                       ignoreSelf;

    public RingCommunications(Context<T> context, SigningMember member, CommonCommunications<Comm, ?> comm) {
        this(context, member, comm, false);
    }

    public RingCommunications(Context<T> context, SigningMember member, CommonCommunications<Comm, ?> comm,
                              boolean ignoreSelf) {
        this.context = context;
        this.member = member;
        this.comm = comm;
        this.ignoreSelf = ignoreSelf;
    }

    public RingCommunications<T, Comm> allowDuplicates() {
        noDuplicates = false;
        return this;
    }

    public void dontIgnoreSelf() {
        this.ignoreSelf = false;
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

    public void ignoreSelf() {
        this.ignoreSelf = true;
    }

    public RingCommunications<T, Comm> noDuplicates() {
        noDuplicates = true;
        return this;
    }

    public void reset() {
        currentIndex = -1;
        traversalOrder.clear();
        log.trace("Reset on: {}", member.getId());
    }

    @Override
    public String toString() {
        return "RingCommunications [" + context.getId() + ":" + member.getId() + ":" + currentIndex + "]";
    }

    final RingCommunications.Destination<T, Comm> next(Digest digest) {
        lock.lock();
        try {
            final var current = currentIndex;
            final var count = traversalOrder.size();
            if (count == 0 || current == count - 1) {
                var successors = context.successors(digest, ignoreSelf ? (T) member : null, noDuplicates, (T) member);
                traversalOrder.clear();
                traversalOrder.addAll(successors);
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
        Context.iteration<T> successor = null;
        try {
            successor = traversalOrder.get(current);
            final Comm link = comm.connect(successor.m());
            if (link == null) {
                log.trace("No connection to {} on: {}", successor.m() == null ? "<null>" : successor.m().getId(),
                          member.getId());
            }
            return new Destination<>(successor.m(), link, successor.ring());
        } catch (Throwable e) {
            log.trace("error opening connection to {}: {} on: {}",
                      successor.m() == null ? "<null>" : successor.m().getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage(), member.getId());
            return new Destination<>(successor.m(), null, successor.ring());
        }
    }

    public record Destination<M, Q>(M member, Q link, int ring) {
    }
}
