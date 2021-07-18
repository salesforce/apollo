/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer;

/**
 * @author hal.hildebrand
 *
 */
public record Config(short nProc, int epochLength, short pid, int zeroVotRoundForCommonVote, int firstDecidedRound,
                     int orderStartLevel, int commonVoteDeterministicPrefix, short crpFixedPrefix, Signer signer,
                     DigestAlgorithm digestAlgorithm, int lastLevel, boolean canSkipLevel, int numberOfEpochs,
                     List<BiConsumer<Unit, Dag>> checks, WeakThresholdKey WTKey, Executor executor) {
    
    public static Config empty() {
        return Builder.empty().build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder builderFrom(Config config) {
        return new Builder(config);
    }

    public static class Builder {
        public static Builder empty() {
            return new Builder().requiredByLinear();
        }

        private boolean                     canSkipLevel    = false;
        private List<BiConsumer<Unit, Dag>> checks;
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
        private short                       pid;
        private Signer                      signer;
        private WeakThresholdKey            wtk;
        private int                         zeroVotRoundForCommonVote;

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
            zeroVotRoundForCommonVote = config.zeroVotRoundForCommonVote;
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
            return new Config(nProc, epochLength, pid, zeroVotRoundForCommonVote, firstDecidedRound, orderStartLevel,
                              commonVoteDeterministicPrefix, crpFixedPrefix, signer, digestAlgorithm, lastLevel,
                              canSkipLevel, numberOfEpochs, checks, wtk, executor);
        }

        public List<BiConsumer<Unit, Dag>> getChecks() {
            return checks;
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
            return zeroVotRoundForCommonVote;
        }

        public boolean isCanSkipLevel() {
            return canSkipLevel;
        }

        public Builder requiredByLinear() {
            firstDecidedRound = 3;
            commonVoteDeterministicPrefix = 10;
            zeroVotRoundForCommonVote = 3;
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

        public Builder setZeroVotRoundForCommonVote(int zeroVotRoundForCommonVote) {
            this.zeroVotRoundForCommonVote = zeroVotRoundForCommonVote;
            return this;
        }
    }

}
