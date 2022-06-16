/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.Unit;

/**
 * @author hal.hildebrand
 *
 */

public record UnanimousVoter(Dag dag, Unit uc, int zeroVoteRoundForCommonVote, int commonVoteDeterministicPrefix,
                             Map<Digest, Vote> votingMemo, String logLabel) {

    private static final Logger log = LoggerFactory.getLogger(UnanimousVoter.class);

    private record R(Vote vote, boolean finished) {}

    public record Decision(Vote decision, int decisionLevel) {}

    private record votingResult(short popular, short unpopular) {}

    public static class SuperMajorityDecider {
        private Vote                 decision = Vote.UNDECIDED;
        private int                  decisionLevel;
        private final UnanimousVoter v;
        private final String logLabel;

        public SuperMajorityDecider(UnanimousVoter v, String logLabel) {
            this.v = v;
            this.logLabel = logLabel;
        }

        public Vote decide(Unit u) {
            var commonVote = v.lazyCommonVote(u.level() - 1);
            var r = v.voteUsingPrimeAncestors(v.uc, u, v.dag, (uc, uPrA) -> {
                short pop = 0;
                short unpop = 0;
                Vote result = v.voteUsing(uPrA);
                if (result == Vote.UNDECIDED) {
                    result = commonVote.get();
                }
                var updated = false;
                switch (result) {
                case POPULAR:
                    pop++;
                    updated = true;
                    break;
                case UNPOPULAR:
                    unpop++;
                    updated = true;
                    break;
                default:
                    break;
                }
                if (updated) {
                    if (superMajority(v.dag, new votingResult(pop, unpop)) != Vote.UNDECIDED) {
                        log.trace("Vote decided: {} for candidate: {} prime ancestor: {} on: {}", result, uc, uPrA, logLabel);
                        return new R(result, true);
                    }
                } else {
                    // fast fail
                    int remaining = v.dag.nProc() - uPrA.creator() - 1;
                    pop += remaining;
                    unpop += remaining;
                    if (superMajority(v.dag, new votingResult(pop, unpop)) == Vote.UNDECIDED) {
                        log.trace("Vote decided: {} for candidate: {} prime ancestor: {} on: {}", result, uc, uPrA, logLabel);
                        return new R(result, true);
                    }
                }

                log.trace("Vote decided: {} for candidate: {} prime ancestor: {} on: {}", result, uc, uPrA, logLabel);
                return new R(result, false);
            });
            final var vote = superMajority(v.dag, r);
            log.trace("Vote decided: {} for candidate: {} on: {}", vote, u, logLabel);
            return vote;
        }

        /**
         * Decides if uc is popular (i.e. it can be used as a timing unit). Returns
         * vote, level on which the decision was made and current dag level.
         */
        public Decision decideUnitIsPopular(int dagMaxLevel) {
            if (decision != Vote.UNDECIDED) {
                return new Decision(decision, decisionLevel);
            }
            int maxDecisionLevel = getMaxDecideLevel(dagMaxLevel);

            for (int level = v.uc.level() + firstVotingRound + 1; level <= maxDecisionLevel; level++) {
                AtomicReference<Vote> decision = new AtomicReference<>(Vote.UNDECIDED);

                var commonVote = v.lazyCommonVote(level);
                v.dag.iterateUnitsOnLevel(level, primes -> {
                    for (var v : primes) {
                        Vote vDecision = decide(v);
                        if (vDecision != Vote.UNDECIDED && vDecision == commonVote.get()) {
                            decision.set(vDecision);
                            return false;
                        }
                    }
                    return true;
                });

                if (decision.get() != Vote.UNDECIDED) {
                    this.decision = decision.get();
                    this.decisionLevel = level;
                    v.dispose();
                    return new Decision(decision.get(), level);
                }
            }

            return new Decision(Vote.UNDECIDED, -1);
        }

        /**
         * returns a maximal level of a prime unit which can be used for deciding
         * assuming that dag is on level 'dagMaxLevel'.
         */
        private int getMaxDecideLevel(int dagMaxLevel) {
            var deterministicLevel = v.uc.level() + v.commonVoteDeterministicPrefix;
            if (dagMaxLevel - 2 < deterministicLevel) {
                if (deterministicLevel > dagMaxLevel) {
                    return dagMaxLevel;
                }
                return deterministicLevel;
            }
            return dagMaxLevel - 2;
        }

        /**
         * Checks if votes for popular or unpopular make a quorum. Returns the vote
         * making a quorum or undecided if there is no quorum.
         */
        private Vote superMajority(Dag dag, votingResult votes) {
            if (dag.isQuorum(votes.popular)) {
                return Vote.POPULAR;
            }
            if (dag.isQuorum(votes.unpopular)) {
                return Vote.UNPOPULAR;
            }
            return Vote.UNDECIDED;
        }
    }

    static final int firstVotingRound = 1;

    private boolean coinToss(Unit uc, int level) {
        return level % 2 == 0;
//        return (rs.randomBytes(uc.creator(), level)[0] & 1) == 0;
    }

    public Vote voteUsing(Unit u) {
        var r = u.level() - uc.level();
        if (r < firstVotingRound) {
            return Vote.UNDECIDED;
        }
        var cachedResult = votingMemo.get(u.hash());
        if (cachedResult != null) {
            return cachedResult;
        }
        AtomicReference<Vote> result = new AtomicReference<>();

        try {
            if (r == firstVotingRound) {
                return initialVote(uc, u);
            }
            var commonVote = lazyCommonVote(u.level() - 1);
            AtomicReference<Vote> lastVote = new AtomicReference<>();
            voteUsingPrimeAncestors(uc, u, dag, (uc, uPrA) -> {
                result.set(voteUsing(uPrA));
                if (result.get() == Vote.UNDECIDED) {
                    result.set(commonVote.get());
                }
                if (lastVote.get() != null) {
                    if (lastVote.get() != result.get()) {
                        log.trace("Undecided, last Vote: {} != result: {} for candidate: {} prime ancestor: {} on: {}",
                                  lastVote.get(), result.get(), uc, u, logLabel);
                        lastVote.set(Vote.UNDECIDED);
                        return new R(result.get(), true);
                    }
                } else {
                    lastVote.set(result.get());
                }
                return new R(result.get(), false);

            });
            if (lastVote.get() == null) {
                log.trace("Undecided, no last vote for candidate: {} prime ancestor: {} on: {}", lastVote.get(), uc, u, logLabel);
                return Vote.UNDECIDED;
            }
            log.trace("Vote result: {} candidate: {} prime ancestor: {} on: {}", lastVote.get(), uc, u, logLabel);
            return lastVote.get();
        } finally {
            votingMemo.put(u.hash(), result.get());
        }
    }

    private Supplier<Vote> lazyCommonVote(int level) {
        AtomicBoolean initialized = new AtomicBoolean();
        AtomicReference<Vote> commonVoteValue = new AtomicReference<>();
        return () -> {
            if (initialized.compareAndSet(false, true)) {
                commonVoteValue.set(commonVote(level));
            }
            return commonVoteValue.get();
        };
    }

    private Vote commonVote(int level) {
        var round = level - uc.level();
        if (round <= firstVotingRound) {
            log.trace("Common vote is asked on too low unit level: {} on: {}", level, logLabel);
            return Vote.UNDECIDED;
        }
        if (round <= commonVoteDeterministicPrefix) {
            if (round == zeroVoteRoundForCommonVote) {
                log.trace("Common vote level: {} is asked on the zero vote round: {} on: {}", level, zeroVoteRoundForCommonVote, logLabel);
                return Vote.UNPOPULAR;
            }
            log.trace("Common vote popular level: {} as round is less than the determinist prefix: {} on: {}", level,
                      commonVoteDeterministicPrefix, logLabel);
            return Vote.POPULAR;
        }
        if (coinToss(uc, level + 1)) {
            log.trace("Common vote popular level: {} due to coin toss on: {}", level, commonVoteDeterministicPrefix, logLabel);
            return Vote.POPULAR;
        }

        log.trace("Common vote unpopular level: {} on: {}", level, logLabel);
        return Vote.UNPOPULAR;
    }

    private Vote initialVote(Unit uc, Unit u) {
        if (u.above(uc)) {
            log.trace("Intial vote popular candidate: {} is above {} on: {}", uc, u, logLabel);
            return Vote.POPULAR;
        } else {
            return Vote.UNPOPULAR;
        }
    }

    private votingResult voteUsingPrimeAncestors(Unit uc, Unit u, Dag dag, BiFunction<Unit, Unit, R> voter) {
        short pop = 0;
        short unpop = 0;
        for (short pid = 0; pid < dag.nProc(); pid++) {
            var floor = u.floor(pid);
            log.trace("Voting pid: {} candidate: {} prime: {} is: {} on: {}", pid, uc, u, floor, logLabel);
            var votesOne = false;
            var votesZero = false;
            var finish = false;
            for (var v : floor) {
                // find prime ancestor
                for (var predecessor = v; predecessor.level() >= u.level() - 1;) {
                    v = predecessor;
                    predecessor = v.predecessor();
                    if (predecessor == null) {
                        break;
                    }
                }
                if (v.level() != u.level() - 1) {
                    continue;
                }

                // compute vote using prime ancestor
                R counted = voter.apply(uc, v);
                switch (counted.vote) {
                case POPULAR:
                    votesOne = true;
                case UNPOPULAR:
                    votesZero = true;
                default:
                    break;
                }
                if (finish || (votesOne && votesZero)) {
                    break;
                }
            }
            if (votesOne) {
                pop++;
            }
            if (votesZero) {
                unpop++;
            }
            if (finish) {
                log.trace("Vote pid: {} pop: {} unpop: {} for candidate: {} prime ancestor: {} on: {}", pid, pop, unpop, uc,
                          u, logLabel);
                return new votingResult(pop, unpop);
            }
        }
        return new votingResult(pop, unpop);
    }

    private void dispose() {
        votingMemo.clear();
    }
}
