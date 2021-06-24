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
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 *
 */
public class RingIterator<Comm extends Link> extends RingCommunications<Comm> {
    private static final Logger log = LoggerFactory.getLogger(RingIterator.class);

    private volatile boolean majorityFailed  = false;
    private volatile boolean majoritySucceed = false;

    public RingIterator(Context<Member> context, SigningMember member, CommonCommunications<Comm, ?> comm,
            Executor executor) {
        super(context, member, comm, executor);
    }

    public RingIterator(Direction direction, Context<Member> context, SigningMember member,
            CommonCommunications<Comm, ?> comm, Executor executor) {
        super(direction, context, member, comm, executor);
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
        executor.execute(() -> internalIterate(digest, onMajority, round, failedMajority, handler, onComplete, tally));

    }

    private <T> void internalIterate(Digest digest, Runnable onMajority,
                                     BiFunction<Comm, Integer, ListenableFuture<T>> round, Runnable failedMajority,
                                     PredicateHandler<T, Comm> handler, Runnable onComplete, AtomicInteger tally) {
        Runnable proceed = () -> internalIterate(digest, onMajority, round, failedMajority, handler, onComplete, tally);
        final int current = lastRingIndex;
        int ringCount = context.getRingCount();
        boolean finalIteration = current % ringCount >= ringCount - 1;
        int majority = context.majority();
        Consumer<Boolean> allowed = allow -> proceed(digest, allow, onMajority, majority, failedMajority, tally,
                                                     proceed, finalIteration, onComplete);
        try (Comm link = nextRing(digest)) {
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

    private void proceed(Digest key, Boolean allow, Runnable onMajority, int majority, Runnable failedMajority,
                         AtomicInteger tally, Runnable proceed, boolean finalIteration, Runnable onComplete) {
        log.trace("Determining continuation of: {} for: {} tally: {} majority: {} final itr: {} allow: {} on: {}", key,
                  context.getId(), tally.get(), majority, finalIteration, allow, member);
        if (onMajority != null && !majoritySucceed) {
            if (tally.get() >= majority) {
                majoritySucceed = true;
                log.debug("Obtained majority of: {} for: {} tally: {} on: {}", key, context.getId(), tally.get(),
                          member);
                onMajority.run();
            }
        }
        if (finalIteration && allow) {
            log.trace("Final iteration of: {} for: {} tally: {} on: {}", context.getId(), tally.get(), member);
            if (failedMajority != null && !majorityFailed) {
                if (tally.get() < majority) {
                    majorityFailed = true;
                    log.debug("Failed to obtain majority of: {} for: {} tally: {} required: {} on: {}", key,
                              context.getId(), tally.get(), majority, member);
                    failedMajority.run();
                }
            }
            if (onComplete != null) {
                log.trace("Completing iteration of: {} for: {} tally: {} on: {}", key, context.getId(), tally.get(),
                          member);
                onComplete.run();
            }
        } else if (allow) {
            log.trace("Proceeding on: {} for: {} tally: {} on: {}", key, context.getId(), tally.get(), member);
            executor.execute(proceed);
        } else {
            log.trace("Termination on: {} for: {} tally: {} on: {}", key, context.getId(), tally.get(), member);
        }
    }
}
