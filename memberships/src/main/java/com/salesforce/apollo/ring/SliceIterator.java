/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ring;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.archipelago.Router.CommonCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class SliceIterator<Comm extends Link> {
    @FunctionalInterface
    public interface SlicePredicateHandler<T, Comm> {
        boolean handle(Optional<ListenableFuture<T>> futureSailor, Comm communications, Member member);
    }

    private static final Logger                 log     = LoggerFactory.getLogger(SliceIterator.class);
    private final CommonCommunications<Comm, ?> comm;
    private AtomicInteger                       current = new AtomicInteger(0);
    private final Executor                      exec;
    private final String                        label;
    private final SigningMember                 member;
    private final List<? extends Member>        slice;

    public SliceIterator(String label, SigningMember member, List<? extends Member> slice,
                         CommonCommunications<Comm, ?> comm, Executor exec) {
        assert member != null && slice != null && comm != null;
        this.label = label;
        this.member = member;
        this.slice = slice;
        this.comm = comm;
        this.exec = exec;
        Entropy.secureShuffle(slice);
    }

    public <T> void iterate(BiFunction<Comm, Member, ListenableFuture<T>> round, SlicePredicateHandler<T, Comm> handler,
                            Runnable onComplete, ScheduledExecutorService scheduler, Duration frequency) {
        internalIterate(round, handler, onComplete, scheduler, frequency);
    }

    public <T> void iterate(BiFunction<Comm, Member, ListenableFuture<T>> round, SlicePredicateHandler<T, Comm> handler,
                            ScheduledExecutorService scheduler, Duration frequency) {
        iterate(round, handler, null, scheduler, frequency);
    }

    private <T> void internalIterate(BiFunction<Comm, Member, ListenableFuture<T>> round,
                                     SlicePredicateHandler<T, Comm> handler, Runnable onComplete,
                                     ScheduledExecutorService scheduler, Duration frequency) {
        Runnable proceed = () -> internalIterate(round, handler, onComplete, scheduler, frequency);

        boolean finalIteration = current.get() % slice.size() >= slice.size() - 1;

        Consumer<Boolean> allowed = allow -> proceed(allow, proceed, finalIteration, onComplete, scheduler, frequency);
        try (Comm link = next()) {
            if (link == null) {
                log.trace("No link found on: {} member: {}  on: {}", label, slice.get(current.get()), member);
                final boolean allow = handler.handle(Optional.empty(), link, slice.get(current.get()));
                allowed.accept(allow);
                return;
            }
            log.trace("Iteration on: {} index: {} to: {} on: {}", label, current.get(), link.getMember(), member);
            ListenableFuture<T> futureSailor = round.apply(link, link.getMember());
            if (futureSailor == null) {
                log.trace("No asynchronous response  on: {} index: {} from: {} on: {}", label, current.get(),
                          link.getMember(), member);
                final boolean allow = handler.handle(Optional.empty(), link, link.getMember());
                allowed.accept(allow);
                return;
            }
            futureSailor.addListener(Utils.wrapped(() -> allowed.accept(handler.handle(Optional.of(futureSailor), link,
                                                                                       link.getMember())),
                                                   log),
                                     exec);
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    private Comm linkFor(int index) {
        try {
            return comm.connect(slice.get(index));
        } catch (Throwable e) {
            log.error("error opening connection to {}: {}", slice.get(index).getId(),
                      (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private Comm next() {
        Comm link = null;
        int c = (current.get() + 1) % slice.size();
        for (int i = 0; i < slice.size(); i++) {
            if (c == 0) {
                Entropy.secureShuffle(slice);
            }
            link = linkFor(c);
            if (link != null) {
                break;
            }
            current.set((c + 1) % slice.size());
        }
        current.set(c);
        return link;
    }

    private void proceed(final boolean allow, Runnable proceed, boolean finalIteration, Runnable onComplete,
                         ScheduledExecutorService scheduler, Duration frequency) {
        log.trace("Determining continuation for: {} final itr: {} allow: {} on: {}", label, finalIteration, allow,
                  member);
        if (finalIteration && allow) {
            log.trace("Final iteration of: {} on: {}", label, member);
            if (onComplete != null) {
                log.trace("Completing iteration for: {} on: {}", label, member);
                onComplete.run();
            }
        } else if (allow) {
            log.trace("Proceeding for: {} on: {}", label, member);
            scheduler.schedule(Utils.wrapped(proceed, log), frequency.toNanos(), TimeUnit.NANOSECONDS);
        } else {
            log.trace("Termination for: {} on: {}", label, member);
        }
    }
}
