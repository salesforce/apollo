/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import java.time.Clock;
import java.util.Objects;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.MockSigner;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.WeakThresholdKey.NoOpWeakThresholdKey;

/**
 * Configuration for an Ethereal instantiation.
 * 
 * @author hal.hildebrand
 *
 */
public record Config(String label, short nProc, int epochLength, short pid, int zeroVoteRoundForCommonVote,
                     int firstDecidedRound, int orderStartLevel, Signer signer, short prefixLength,
                     DigestAlgorithm digestAlgorithm, int lastLevel, boolean canSkipLevel, int numberOfEpochs,
                     WeakThresholdKey WTKey, Clock clock, double bias, Verifier[] verifiers, double fpr) {

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

    public String logLabel() {
        return label + "(" + pid + ")";
    }

    public static class Builder implements Cloneable {
        public static Builder empty() {
            return new Builder().requiredByLinear();
        }

        private int              bias            = 3;
        private boolean          canSkipLevel    = true;
        private Clock            clock           = Clock.systemUTC();
        private DigestAlgorithm  digestAlgorithm = DigestAlgorithm.DEFAULT;
        private int              epochLength     = 30;
        private int              firstDecidedRound;
        private double           fpr             = 0.125;
        private String           label           = "";
        private int              lastLevel       = -1;
        private short            nProc;
        private int              numberOfEpochs  = 3;
        private int              orderStartLevel = 6;
        private double           pByz            = -1;
        private short            pid;
        private short            prefixLength    = -1;
        private Signer           signer          = new MockSigner(SignatureAlgorithm.DEFAULT);
        private Verifier[]       verifiers;
        private WeakThresholdKey wtk;
        private int              zeroVoteRoundForCommonVote;

        public Builder() {
        }

        public Builder(Config config) {
            canSkipLevel = config.canSkipLevel;
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
            numberOfEpochs = 3;
            epochLength = 30;
            return this;
        }

        public Builder addLastLevel() {
            lastLevel = epochLength + orderStartLevel - 1;
            return this;
        }

        public Builder addSetUpConfig() {
            canSkipLevel = false;
            orderStartLevel = 6;
            epochLength = 1;
            numberOfEpochs = 1;
            return this;
        }

        public Config build() {
            if (prefixLength <= 0) {
                prefixLength = Dag.minimalTrusted(nProc);
            }
            if (pByz <= -1) {
                pByz = 1.0 / bias;
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
            return new Config(label, nProc, epochLength, pid, zeroVoteRoundForCommonVote, firstDecidedRound,
                              orderStartLevel, signer, prefixLength, digestAlgorithm, lastLevel, canSkipLevel,
                              numberOfEpochs, wtk, clock, bias, verifiers, fpr);
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

        public Clock getClock() {
            return clock;
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

        public String getLabel() {
            return label;
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

        public short getPrefixLength() {
            return prefixLength;
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

        public Builder setClock(Clock clock) {
            this.clock = clock;
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

        public Builder setLabel(String label) {
            this.label = label;
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

        public Builder setPrefixLength(short prefixLength) {
            this.prefixLength = prefixLength;
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
