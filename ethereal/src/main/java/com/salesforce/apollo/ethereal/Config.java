/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.MockSigner;
import com.salesforce.apollo.ethereal.Ethereal.Committee;
import com.salesforce.apollo.ethereal.Ethereal.Committee.Default;
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
                     List<BiConsumer<Unit, Dag>> checks, WeakThresholdKey WTKey, Executor executor, int byzantine,
                     Committee committee, Clock clock) {

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

        private int                         byzantine       = -1;
        private boolean                     canSkipLevel    = false;
        private List<BiConsumer<Unit, Dag>> checks;
        private Clock                       clock           = Clock.systemUTC();
        private Committee                   committee       = new Default(Collections.emptyMap());
        private int                         commonVoteDeterministicPrefix;
        private short                       crpFixedPrefix;
        private DigestAlgorithm             digestAlgorithm = DigestAlgorithm.DEFAULT;
        private int                         epochLength     = 1;
        private Executor                    executor        = r -> r.run();
        private int                         firstDecidedRound;
        private int                         lastLevel;
        private short                       nProc;
        private int                         numberOfEpochs  = 1;
        private int                         orderStartLevel = 6;
        private double                      pByz            = 0.33;
        private short                       pid;
        private Signer                      signer          = new MockSigner();
        private WeakThresholdKey            wtk;
        private int                         zeroVoteRoundForCommonVote;

        public Builder() {
        }

        public Builder(Config config) {
            canSkipLevel = config.canSkipLevel;
            checks = config.checks;
            commonVoteDeterministicPrefix = config.commonVoteDeterministicPrefix;
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
            canSkipLevel = true;
            orderStartLevel = 0;
            crpFixedPrefix = 4;
            numberOfEpochs = 3;
            epochLength = 30;
            checks = Checks.ConsensusChecks;
            return this;
        }

        public Builder addSetUpConfig() {
            canSkipLevel = false;
            orderStartLevel = 6;
            crpFixedPrefix = 0;
            epochLength = 1;
            numberOfEpochs = 1;
            checks = Checks.SetupChecks;
            return this;
        }

        public Config build() {
            if (byzantine <= -1) {
                byzantine = (int) ((nProc) * pByz);
            }
            if (wtk == null) {
                wtk = new NoOpWeakThresholdKey((2 * byzantine) + 1);
            }
            lastLevel = epochLength + orderStartLevel;
            Objects.requireNonNull(committee, "Committee cannot be null");
            Objects.requireNonNull(signer, "Signer cannot be null");
            Objects.requireNonNull(digestAlgorithm, "Digest Algorithm cannot be null");

            return new Config(nProc, epochLength, pid, zeroVoteRoundForCommonVote, firstDecidedRound, orderStartLevel,
                              commonVoteDeterministicPrefix, crpFixedPrefix, signer, digestAlgorithm, lastLevel,
                              canSkipLevel, numberOfEpochs, checks, wtk, executor, byzantine, committee, clock);
        }

        @Override
        public Builder clone() {
            try {
                return (Builder) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new IllegalStateException(e);
            }
        }

        public int getByzantine() {
            return byzantine;
        }

        public List<BiConsumer<Unit, Dag>> getChecks() {
            return checks;
        }

        public Clock getClock() {
            return clock;
        }

        public Committee getCommittee() {
            return committee;
        }

        public int getCommonVoteDeterministicPrefix() {
            return commonVoteDeterministicPrefix;
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

        public Executor getExecutor() {
            return executor;
        }

        public int getFirstDecidedRound() {
            return firstDecidedRound;
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
            commonVoteDeterministicPrefix = 10;
            zeroVoteRoundForCommonVote = 3;
            return this;
        }

        public Builder setByzantine(int byzantine) {
            this.byzantine = byzantine;
            return this;
        }

        public Builder setCanSkipLevel(boolean canSkipLevel) {
            this.canSkipLevel = canSkipLevel;
            return this;
        }

        public Builder setChecks(List<BiConsumer<Unit, Dag>> checks) {
            this.checks = checks;
            return this;
        }

        public Builder setClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder setCommittee(Committee committee) {
            this.committee = committee;
            return this;
        }

        public Builder setCommonVoteDeterministicPrefix(int commonVoteDeterministicPrefix) {
            this.commonVoteDeterministicPrefix = commonVoteDeterministicPrefix;
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

        public Builder setExecutor(Executor executor) {
            this.executor = executor;
            return this;
        }

        public Builder setFirstDecidedRound(int firstDecidedRound) {
            this.firstDecidedRound = firstDecidedRound;
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
