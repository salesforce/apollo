/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.MockSigner;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.Adder.Correctness;
import com.salesforce.apollo.ethereal.WeakThresholdKey.NoOpWeakThresholdKey;

/**
 * Configuration for an Ethereal instantiation.
 * 
 * @author hal.hildebrand
 *
 */
public record Config(short nProc, int epochLength, short pid, int zeroVoteRoundForCommonVote, int firstDecidedRound,
                     int orderStartLevel, int commonVoteDeterministicPrefix, short crpFixedPrefix, Signer signer,
                     DigestAlgorithm digestAlgorithm, int lastLevel, boolean canSkipLevel, int numberOfEpochs,
                     List<BiFunction<Unit, Dag, Correctness>> checks, WeakThresholdKey WTKey, Clock clock, double bias,
                     Verifier[] verifiers, double fpr) {

    public static Builder deterministic() {
        Builder b = new Builder();
        b.requiredByLinear();
        b.addConsensusConfig();
        return b;
    }

    public static Config empty() {
        return Builder.empty().build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder builderFrom(Config config) {
        return new Builder(config);
    }

    public static class Builder implements Cloneable {
        public static Builder empty() {
            return new Builder().requiredByLinear();
        }

        private int                                      bias            = 3;
        private boolean                                  canSkipLevel    = false;
        private List<BiFunction<Unit, Dag, Correctness>> checks          = new ArrayList<>();
        private Clock                                    clock           = Clock.systemUTC();
        private short                                    crpFixedPrefix;
        private DigestAlgorithm                          digestAlgorithm = DigestAlgorithm.DEFAULT;
        private int                                      epochLength     = 30;
        private int                                      firstDecidedRound;
        private double                                   fpr             = 0.125;
        private int                                      lastLevel       = -1;
        private short                                    nProc;
        private int                                      numberOfEpochs  = 3;
        private int                                      orderStartLevel = 6;
        private double                                   pByz            = -1;
        private short                                    pid;
        private Signer                                   signer          = new MockSigner();
        private Verifier[]                               verifiers;
        private WeakThresholdKey                         wtk;
        private int                                      zeroVoteRoundForCommonVote;

        public Builder() {
        }

        public Builder(Config config) {
            canSkipLevel = config.canSkipLevel;
            checks = config.checks;
            crpFixedPrefix = config.crpFixedPrefix;
            digestAlgorithm = config.digestAlgorithm;
            epochLength = config.epochLength;
            firstDecidedRound = config.firstDecidedRound;
            lastLevel = config.lastLevel;
            nProc = config.nProc;
            numberOfEpochs = config.numberOfEpochs;
            orderStartLevel = config.orderStartLevel;
            pid = config.pid;
            signer = config.signer;
            zeroVoteRoundForCommonVote = config.zeroVoteRoundForCommonVote;
        }

        public Builder addConsensusConfig() {
            canSkipLevel = false;
            orderStartLevel = 0;
            crpFixedPrefix = 4;
            numberOfEpochs = 3;
            epochLength = 30;
            checks.addAll(Checks.ConsensusChecks);
            return this;
        }

        public Builder addLastLevel() {
            lastLevel = epochLength + orderStartLevel - 1;
            return this;
        }

        public Builder addSetUpConfig() {
            canSkipLevel = false;
            orderStartLevel = 6;
            crpFixedPrefix = 0;
            epochLength = 1;
            numberOfEpochs = 1;
            checks.addAll(Checks.SetupChecks);
            return this;
        }

        public Config build() {
            if (pByz <= -1) {
                pByz = 1.0 / (double) bias;
            }
            final var minimalQuorum = Dag.minimalQuorum(nProc, bias);
            if (wtk == null) {
                wtk = new NoOpWeakThresholdKey(minimalQuorum + 1);
            }
            Objects.requireNonNull(signer, "Signer cannot be null");
            Objects.requireNonNull(digestAlgorithm, "Digest Algorithm cannot be null");
            if (lastLevel <= 0) {
                addLastLevel();
            }
            return new Config(nProc, epochLength, pid, zeroVoteRoundForCommonVote, firstDecidedRound, orderStartLevel,
                              10, crpFixedPrefix, signer, digestAlgorithm, lastLevel, canSkipLevel, numberOfEpochs,
                              checks, wtk, clock, bias, verifiers, fpr);
        }

        @Override
        public Builder clone() {
            try {
                return (Builder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new IllegalStateException(e);
            }
        }

        public int getBias() {
            return bias;
        }

        public List<BiFunction<Unit, Dag, Correctness>> getChecks() {
            return checks;
        }

        public Clock getClock() {
            return clock;
        }

        public short getCrpFixedPrefix() {
            return crpFixedPrefix;
        }

        public DigestAlgorithm getDigestAlgorithm() {
            return digestAlgorithm;
        }

        public int getEpochLength() {
            return epochLength;
        }

        public int getFirstDecidedRound() {
            return firstDecidedRound;
        }

        public double getFpr() {
            return fpr;
        }

        public int getLastLevel() {
            return lastLevel;
        }

        public short getnProc() {
            return nProc;
        }

        public int getNumberOfEpochs() {
            return numberOfEpochs;
        }

        public int getOrderStartLevel() {
            return orderStartLevel;
        }

        public double getpByz() {
            return pByz;
        }

        public short getPid() {
            return pid;
        }

        public Signer getSigner() {
            return signer;
        }

        public Verifier[] getVerifiers() {
            return verifiers;
        }

        public WeakThresholdKey getWtk() {
            return wtk;
        }

        public int getZeroVotRoundForCommonVote() {
            return zeroVoteRoundForCommonVote;
        }

        public boolean isCanSkipLevel() {
            return canSkipLevel;
        }

        public Builder requiredByLinear() {
            firstDecidedRound = 3;
            zeroVoteRoundForCommonVote = 3;
            return this;
        }

        public Builder setBias(int bias) {
            this.bias = bias;
            return this;
        }

        public Builder setCanSkipLevel(boolean canSkipLevel) {
            this.canSkipLevel = canSkipLevel;
            return this;
        }

        public Builder setChecks(List<BiFunction<Unit, Dag, Correctness>> checks) {
            this.checks = checks;
            return this;
        }

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder setCrpFixedPrefix(short crpFixedPrefix) {
            this.crpFixedPrefix = crpFixedPrefix;
            return this;
        }

        public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
            return this;
        }

        public Builder setEpochLength(int epochLength) {
            this.epochLength = epochLength;
            return this;
        }

        public Builder setFirstDecidedRound(int firstDecidedRound) {
            this.firstDecidedRound = firstDecidedRound;
            return this;
        }

        public Builder setFpr(double fpr) {
            this.fpr = fpr;
            return this;
        }

        public Builder setLastLevel(int lastLevel) {
            this.lastLevel = lastLevel;
            return this;
        }

        public Builder setnProc(short nProc) {
            this.nProc = nProc;
            return this;
        }

        public Builder setNumberOfEpochs(int numberOfEpochs) {
            this.numberOfEpochs = numberOfEpochs;
            return this;
        }

        public Builder setOrderStartLevel(int orderStartLevel) {
            this.orderStartLevel = orderStartLevel;
            return this;
        }

        public Builder setpByz(double pByz) {
            this.pByz = pByz;
            return this;
        }

        public Builder setPid(short pid) {
            this.pid = pid;
            return this;
        }

        public Builder setSigner(Signer signer) {
            this.signer = signer;
            return this;
        }

        public Builder setVerifiers(Verifier[] verifiers) {
            this.verifiers = verifiers;
            return this;
        }

        public Builder setWtk(WeakThresholdKey wtk) {
            this.wtk = wtk;
            return this;
        }

        public Builder setZeroVoteRoundForCommonVote(int zeroVoteRoundForCommonVote) {
            this.zeroVoteRoundForCommonVote = zeroVoteRoundForCommonVote;
            return this;
        }
    }

}
