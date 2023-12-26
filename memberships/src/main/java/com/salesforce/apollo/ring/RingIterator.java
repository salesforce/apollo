/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ring;

import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.archipelago.RouterImpl.CommonCommunications;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * @author hal.hildebrand
 */
public class RingIterator<T extends Member, Comm extends Link> extends RingCommunications<T, Comm> {
    private static final Logger log = LoggerFactory.getLogger(RingIterator.class);

    private final    Duration                 frequency;
    private final    ScheduledExecutorService scheduler;
    private volatile boolean                  majorityFailed  = false;
    private volatile boolean                  majoritySucceed = false;

    public RingIterator(Duration frequency, Context<T> context, SigningMember member,
                        CommonCommunications<Comm, ?> comm, boolean ignoreSelf, ScheduledExecutorService scheduler) {
        super(context, member, comm, ignoreSelf);
        this.scheduler = scheduler;
        this.frequency = frequency;
    }

    public RingIterator(Duration frequency, Context<T> context, SigningMember member,
                        ScheduledExecutorService scheduler, CommonCommunications<Comm, ?> comm) {
        this(frequency, context, member, comm, false, scheduler);
    }

    public RingIterator(Duration frequency, Direction direction, Context<T> context, SigningMember member,
                        CommonCommunications<Comm, ?> comm, boolean ignoreSelf, ScheduledExecutorService scheduler) {
        super(direction, context, member, comm, ignoreSelf);
        this.scheduler = scheduler;
        this.frequency = frequency;
    }

    public RingIterator(Duration frequency, Direction direction, Context<T> context, SigningMember member,
                        ScheduledExecutorService scheduler, CommonCommunications<Comm, ?> comm) {
        this(frequency, direction, context, member, comm, false, scheduler);
    }

    public <Q> void iterate(Digest digest, BiFunction<Comm, Integer, Q> round, ResultConsumer<T, Q, Comm> handler) {
        iterate(digest, null, round, null, handler, null);
    }

    public <Q> void iterate(Digest digest, BiFunction<Comm, Integer, Q> round, ResultConsumer<T, Q, Comm> handler,
                            Consumer<Integer> onComplete) {
        iterate(digest, null, round, null, handler, onComplete);
    }

    public <Q> void iterate(Digest digest, Runnable onMajority, BiFunction<Comm, Integer, Q> round,
                            Runnable failedMajority, ResultConsumer<T, Q, Comm> handler, Consumer<Integer> onComplete) {
        AtomicInteger tally = new AtomicInteger(0);
        var traversed = new ConcurrentSkipListSet<Member>();
        Thread.ofVirtual()
              .factory()
              .newThread(
              () -> internalIterate(digest, onMajority, round, failedMajority, handler, onComplete, tally, traversed))
              .start();

    }

    public int iteration() {
        return currentIndex + 1;
    }

    @Override
    public RingIterator<T, Comm> noDuplicates() {
        super.noDuplicates();
        return this;
    }

    private <Q> void internalIterate(Digest digest, Runnable onMajority, BiFunction<Comm, Integer, Q> round,
                                     Runnable failedMajority, ResultConsumer<T, Q, Comm> handler,
                                     Consumer<Integer> onComplete, AtomicInteger tally, Set<Member> traversed) {

        Runnable proceed = () -> internalIterate(digest, onMajority, round, failedMajority, handler, onComplete, tally,
                                                 traversed);
        boolean completed = currentIndex == context.getRingCount() - 1;

        Consumer<Boolean> allowed = allow -> proceed(digest, allow, onMajority, failedMajority, tally, completed,
                                                     onComplete);
        if (completed) {
            allowed.accept(true);
            return;
        }

        var next = next(digest);
        log.trace("Iteration: {} tally: {} for digest: {} on: {} ring: {} complete: false on: {}", iteration(),
                  tally.get(), digest, context.getId(), next.ring(), member.getId());
        if (next.link() == null) {
            log.trace("No successor found for digest: {} on: {} iteration: {} traversed: {} ring: {} on: {}", digest,
                      context.getId(), iteration(), traversed, context.ring(currentIndex).stream().toList(),
                      member.getId());
            final boolean allow = handler.handle(tally, Optional.empty(), next);
            allowed.accept(allow);
            if (allow) {
                log.trace("Finished on iteration: {} proceeding on: {} for digest: {} tally: {} on: {}", iteration(),
                          digest, context.getId(), tally.get(), member.getId());
                schedule(proceed);
            } else {
                log.trace("Completed on iteration: {} on: {} for digest: {} for: {} tally: {} on: {}", iteration(),
                          digest, context.getId(), tally.get(), member.getId());
            }
            return;
        }
        try (Comm link = next.link()) {
            log.trace("Continuation on iteration: {} tally: {} for digest: {} on: {} ring: {} to: {} on: {}",
                      iteration(), tally.get(), digest, context.getId(), next.ring(),
                      link.getMember() == null ? null : link.getMember().getId(), member.getId());
            Q result = null;
            try {
                result = round.apply(link, next.ring());
            } catch (Throwable e) {
                log.trace("Exception in round for digest: {} context: {} iteration: {} from: {} on: {}", digest,
                          context.getId(), iteration(), link.getMember() == null ? null : link.getMember().getId(),
                          member.getId(), e);
            }
            if (result == null) {
                log.trace("No asynchronous response for digest: {} on: {} iteration: {} from: {} on: {}", digest,
                          context.getId(), iteration(), link.getMember() == null ? null : link.getMember().getId(),
                          member.getId());
                final boolean allow = handler.handle(tally, Optional.empty(), next);
                allowed.accept(allow);
                if (allow) {
                    log.trace("Proceeding on iteration: {} on: {} for digest: {} tally: {} on: {}", iteration(), digest,
                              context.getId(), tally.get(), member.getId());
                    schedule(proceed);
                } else {
                    log.trace("Completed on iteration: {} on: {} for digest: {} tally: {} on: {}", iteration(), digest,
                              context.getId(), tally.get(), member.getId());
                }
                return;
            }
            final var allow = handler.handle(tally, Optional.of(result), next);
            allowed.accept(allow);
            if (allow) {
                log.trace("Scheduling next iteration: {} on: {} for digest: {} tally: {} on: {}", iteration(), digest,
                          context.getId(), tally.get(), member.getId());
                schedule(proceed);
            } else {
                log.trace("Finished on iteration: {} on: {} for digest: {} tally: {} on: {}", iteration(), digest,
                          context.getId(), tally.get(), member.getId());
            }
        } catch (IOException e) {
            log.debug("Error closing", e);
        }
    }

    private void proceed(Digest key, final boolean allow, Runnable onMajority, Runnable failedMajority,
                         AtomicInteger tally, boolean finalIteration, Consumer<Integer> onComplete) {
        final var current = currentIndex;
        if (!finalIteration) {
            log.trace(
            "Determining: {} continuation of: {} for digest: {} tally: {} majority: {} final itr: {} allow: {} on: {}",
            current, key, context.getId(), tally.get(), context.majority(), finalIteration, allow, member.getId());
        }
        if (finalIteration && allow) {
            log.trace("Completing iteration: {} of: {} for digest: {} tally: {} on: {}", iteration(), key,
                      context.getId(), tally.get(), member.getId());
            if (failedMajority != null && !majorityFailed) {
                if (tally.get() < context.majority()) {
                    majorityFailed = true;
                    log.debug("Failed to obtain majority of: {} for digest: {} tally: {} required: {} on: {}", key,
                              context.getId(), tally.get(), context.majority(), member.getId());
                    failedMajority.run();
                }
            }
            if (onComplete != null) {
                onComplete.accept(tally.get());
            }
        } else if (!allow) {
            log.trace("Termination of: {} for digest: {} tally: {} on: {}", key, context.getId(), tally.get(),
                      member.getId());
        } else {
            if (onMajority != null && !majoritySucceed) {
                if (tally.get() >= context.majority()) {
                    majoritySucceed = true;
                    log.debug("Obtained: {} majority of: {} for digest: {} tally: {} on: {}", current, key,
                              context.getId(), tally.get(), member.getId());
                    onMajority.run();
                }
            }
        }
    }

    private void schedule(Runnable proceed) {
        scheduler.schedule(Utils.wrapped(proceed, log), frequency.toNanos(), TimeUnit.NANOSECONDS);
    }
}
