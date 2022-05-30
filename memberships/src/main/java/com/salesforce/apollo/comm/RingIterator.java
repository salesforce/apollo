/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.comm;

import java.io.IOException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
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
import com.salesforce.apollo.membership.Ring.IterateResult;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 *
 */
public class RingIterator<Comm extends Link> extends RingCommunications<Comm> {
    private static final Logger log = LoggerFactory.getLogger(RingIterator.class);

    private final boolean    ignoreSelf;
    private volatile boolean majorityFailed  = false;
    private volatile boolean majoritySucceed = false;

    public RingIterator(Context<Member> context, SigningMember member, CommonCommunications<Comm, ?> comm,
                        Executor exec) {
        this(context, member, comm, exec, false);
    }

    public RingIterator(Context<Member> context, SigningMember member, CommonCommunications<Comm, ?> comm,
                        Executor exec, boolean ignoreSelf) {
        super(context, member, comm, exec);
        this.ignoreSelf = ignoreSelf;
    }

    public RingIterator(Direction direction, Context<Member> context, SigningMember member,
                        CommonCommunications<Comm, ?> comm, Executor exec) {
        this(direction, context, member, comm, exec, false);
    }

    public RingIterator(Direction direction, Context<Member> context, SigningMember member,
                        CommonCommunications<Comm, ?> comm, Executor exec, boolean ignoreSelf) {
        super(direction, context, member, comm, exec);
        this.ignoreSelf = ignoreSelf;
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
        exec.execute(() -> internalIterate(digest, onMajority, round, failedMajority, handler, onComplete, tally,
                                           new HashSet<>()));

    }

    @Override
    protected Logger getLog() {
        return log;
    }

    private <T> void internalIterate(Digest digest, Runnable onMajority,
                                     BiFunction<Comm, Integer, ListenableFuture<T>> round, Runnable failedMajority,
                                     PredicateHandler<T, Comm> handler, Runnable onComplete, AtomicInteger tally,
                                     Set<Member> traversed) {

        Runnable proceed = () -> internalIterate(digest, onMajority, round, failedMajority, handler, onComplete, tally,
                                                 traversed);
        int ringCount = context.getRingCount();

        final var next = nextRing(digest, m -> {
            if (ignoreSelf && member.equals(m)) {
                return IterateResult.CONTINUE;
            }
            if (!context.isActive(m)) {
                return IterateResult.CONTINUE;
            }
            return traversed.add(m) ? IterateResult.SUCCESS : IterateResult.FAIL;
        });
        try (Comm link = next.link()) {
            final int current = lastRingIndex();
            boolean finalIteration = current == ringCount - 1;

            Consumer<Boolean> allowed = allow -> proceed(digest, allow, onMajority, failedMajority, tally,
                                                         finalIteration, onComplete);
            if (finalIteration) {
                traversed.clear();
            }
            if (link == null) {
//                log.trace("No successor found of: {} on: {} ring: {}  on: {}", digest, context.getId(), current,
//                          member);
                final boolean allow = handler.handle(tally, Optional.empty(), link, current);
                allowed.accept(allow);
                if (!finalIteration && allow) {
                    log.trace("Proceeding on: {} for: {} tally: {} on: {}", digest, context.getId(), tally.get(),
                              member.getId());
                    exec.execute(proceed);
                }
                return;
            }
            log.trace("Iteration: {} tally: {} for: {} on: {} ring: {} to: {} on: {}", current, tally.get(), digest,
                      context.getId(), next.ring(), link.getMember() == null ? null : link.getMember().getId(),
                      member.getId());
            ListenableFuture<T> futureSailor = round.apply(link, next.ring());
            if (futureSailor == null) {
                log.trace("No asynchronous response for: {} on: {} ring: {} from: {} on: {}", digest, context.getId(),
                          current, link.getMember() == null ? null : link.getMember().getId(), member.getId());
                final boolean allow = handler.handle(tally, Optional.empty(), link, next.ring());
                allowed.accept(allow);
                if (!finalIteration && allow) {
                    log.trace("Proceeding on: {} for: {} tally: {} on: {}", digest, context.getId(), tally.get(),
                              member.getId());
                    exec.execute(proceed);
                }
                return;
            }
            futureSailor.addListener(() -> {
                final var allow = handler.handle(tally, Optional.of(futureSailor), link, next.ring());
                allowed.accept(allow);
                if (!finalIteration && allow) {
                    log.trace("Proceeding on: {} for: {} tally: {} on: {}", digest, context.getId(), tally.get(),
                              member.getId());
                    exec.execute(proceed);
                }
            }, r -> r.run());
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    private void proceed(Digest key, final boolean allow, Runnable onMajority, Runnable failedMajority,
                         AtomicInteger tally, boolean finalIteration, Runnable onComplete) {
        log.trace("Determining continuation of: {} for: {} tally: {} majority: {} final itr: {} allow: {} on: {}", key,
                  context.getId(), tally.get(), context.majority(), finalIteration, allow, member.getId());
        if (onMajority != null && !majoritySucceed) {
            if (tally.get() >= context.majority()) {
                majoritySucceed = true;
                log.debug("Obtained majority of: {} for: {} tally: {} on: {}", key, context.getId(), tally.get(),
                          member.getId());
                onMajority.run();
            }
        }
        if (finalIteration && allow) {
            log.trace("Final iteration of: {} for: {} tally: {} on: {}", key, context.getId(), tally.get(),
                      member.getId());
            if (failedMajority != null && !majorityFailed) {
                if (tally.get() < context.majority()) {
                    majorityFailed = true;
                    log.debug("Failed to obtain majority of: {} for: {} tally: {} required: {} on: {}", key,
                              context.getId(), tally.get(), context.majority(), member);
                    failedMajority.run();
                }
            }
            if (onComplete != null) {
                log.trace("Completing iteration of: {} for: {} tally: {} on: {}", key, context.getId(), tally.get(),
                          member.getId());
                onComplete.run();
            }
        } else if (!allow) {
            log.trace("Termination on: {} for: {} tally: {} on: {}", key, context.getId(), tally.get(), member.getId());
        }
    }
}
