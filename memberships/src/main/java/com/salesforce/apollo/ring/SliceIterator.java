/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ring;

import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Entropy;
import com.salesforce.apollo.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author hal.hildebrand
 */
public class SliceIterator<Comm extends Link> {
    private static final Logger log = LoggerFactory.getLogger(SliceIterator.class);
    private final CommonCommunications<Comm, ?> comm;
    private final Executor exec;
    private final String label;
    private final SigningMember member;
    private final List<? extends Member> slice;
    private Member current;
    private Iterator<? extends Member> currentIteration;

    public SliceIterator(String label, SigningMember member, List<? extends Member> slice,
                         CommonCommunications<Comm, ?> comm, Executor exec) {
        assert member != null && slice != null && comm != null;
        this.label = label;
        this.member = member;
        this.slice = slice;
        this.comm = comm;
        this.exec = exec;
        Entropy.secureShuffle(slice);
        this.currentIteration = slice.iterator();
        log.debug("Slice: {}", slice.stream().map(m -> m.getId()).toList());
    }

    public <T> void iterate(BiFunction<Comm, Member, T> round, SlicePredicateHandler<T, Comm> handler,
                            Runnable onComplete, ScheduledExecutorService scheduler, Duration frequency) {
        internalIterate(round, handler, onComplete, scheduler, frequency);
    }

    public <T> void iterate(BiFunction<Comm, Member, T> round, SlicePredicateHandler<T, Comm> handler,
                            ScheduledExecutorService scheduler, Duration frequency) {
        iterate(round, handler, null, scheduler, frequency);
    }

    private <T> void internalIterate(BiFunction<Comm, Member, T> round,
                                     SlicePredicateHandler<T, Comm> handler, Runnable onComplete,
                                     ScheduledExecutorService scheduler, Duration frequency) {
        Runnable proceed = () -> internalIterate(round, handler, onComplete, scheduler, frequency);

        Consumer<Boolean> allowed = allow -> proceed(allow, proceed, onComplete, scheduler, frequency);
        try (Comm link = next()) {
            if (link == null) {
                allowed.accept(handler.handle(Optional.empty(), link, slice.get(slice.size() - 1)));
                return;
            }
            log.trace("Iteration on: {} index: {} to: {} on: {}", label, current.getId(), link.getMember(),
                    member.getId());
            var result = round.apply(link, link.getMember());
            allowed.accept(handler.handle(Optional.ofNullable(result), link, link.getMember()));
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    private Comm linkFor(Member m) {
        try {
            return comm.connect(m);
        } catch (Throwable e) {
            log.error("error opening connection to {}: {}", m.getId(),
                    (e.getCause() != null ? e.getCause() : e).getMessage());
        }
        return null;
    }

    private Comm next() {
        if (!currentIteration.hasNext()) {
            Entropy.secureShuffle(slice);
            currentIteration = slice.iterator();
        }
        current = currentIteration.next();
        return linkFor(current);
    }

    private void proceed(final boolean allow, Runnable proceed, Runnable onComplete, ScheduledExecutorService scheduler,
                         Duration frequency) {
        log.trace("Determining continuation for: {} final itr: {} allow: {} on: {}", label, !currentIteration.hasNext(),
                allow, member.getId());
        if (!currentIteration.hasNext() && allow) {
            log.trace("Final iteration of: {} on: {}", label, member.getId());
            if (onComplete != null) {
                log.trace("Completing iteration for: {} on: {}", label, member.getId());
                onComplete.run();
            }
        } else if (allow) {
            log.trace("Proceeding for: {} on: {}", label, member.getId());
            scheduler.schedule(Utils.wrapped(proceed, log), frequency.toNanos(), TimeUnit.NANOSECONDS);
        } else {
            log.trace("Termination for: {} on: {}", label, member.getId());
        }
    }

    @FunctionalInterface
    public interface SlicePredicateHandler<T, Comm> {
        boolean handle(Optional<T> result, Comm communications, Member member);
    }
}
