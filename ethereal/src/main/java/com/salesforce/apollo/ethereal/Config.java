/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Signer;
import com.salesforce.apollo.crypto.Signer.MockSigner;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.ethereal.WeakThresholdKey.NoOpWeakThresholdKey;
import com.salesforce.apollo.membership.Context;
import org.joou.ULong;

import java.util.Objects;

/**
 * Configuration for an Ethereal instantiation.
 *
 * @author hal.hildebrand
 */
public record Config(String label, short nProc, int epochLength, short pid, Signer signer,
                     DigestAlgorithm digestAlgorithm, int lastLevel, int numberOfEpochs, WeakThresholdKey WTKey,
                     double bias, Verifier[] verifiers, double fpr) {

    public static Builder newBuilder() {
        return new Builder();
    }

    public String logLabel() {
        return label + "(" + pid + ")";
    }

    public static class Builder implements Cloneable {

        private int              bias            = 3;
        private DigestAlgorithm  digestAlgorithm = DigestAlgorithm.DEFAULT;
        private int              epochLength     = 30;
        private double           fpr             = 0.0125;
        private String           label           = "";
        private short            nProc;
        private int              numberOfEpochs  = 3;
        private double           pByz            = -1;
        private short            pid;
        private Signer           signer          = new MockSigner(SignatureAlgorithm.DEFAULT, ULong.MIN);
        private Verifier[]       verifiers;
        private WeakThresholdKey wtk;

        public Builder() {
        }

        public Config build() {
            if (pByz <= -1) {
                pByz = 1.0 / bias;
            }
            final var minimalQuorum = Context.minimalQuorum(nProc, bias);
            if (wtk == null) {
                wtk = new NoOpWeakThresholdKey(minimalQuorum + 1);
            }
            Objects.requireNonNull(signer, "Signer cannot be null");
            Objects.requireNonNull(digestAlgorithm, "Digest Algorithm cannot be null");
            return new Config(label, nProc, epochLength, pid, signer, digestAlgorithm, epochLength - 1, numberOfEpochs,
                              wtk, bias, verifiers, fpr);
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

        public Builder setBias(int bias) {
            this.bias = bias;
            return this;
        }

        public DigestAlgorithm getDigestAlgorithm() {
            return digestAlgorithm;
        }

        public Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
            return this;
        }

        public int getEpochLength() {
            return epochLength;
        }

        public Builder setEpochLength(int epochLength) {
            this.epochLength = epochLength;
            return this;
        }

        public double getFpr() {
            return fpr;
        }

        public Builder setFpr(double fpr) {
            this.fpr = fpr;
            return this;
        }

        public String getLabel() {
            return label;
        }

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public short getnProc() {
            return nProc;
        }

        public Builder setnProc(short nProc) {
            this.nProc = nProc;
            return this;
        }

        public int getNumberOfEpochs() {
            return numberOfEpochs;
        }

        public Builder setNumberOfEpochs(int numberOfEpochs) {
            this.numberOfEpochs = numberOfEpochs;
            return this;
        }

        public double getpByz() {
            return pByz;
        }

        public Builder setpByz(double pByz) {
            this.pByz = pByz;
            return this;
        }

        public short getPid() {
            return pid;
        }

        public Builder setPid(short pid) {
            this.pid = pid;
            return this;
        }

        public Signer getSigner() {
            return signer;
        }

        public Builder setSigner(Signer signer) {
            this.signer = signer;
            return this;
        }

        public Verifier[] getVerifiers() {
            return verifiers;
        }

        public Builder setVerifiers(Verifier[] verifiers) {
            this.verifiers = verifiers;
            return this;
        }

        public WeakThresholdKey getWtk() {
            return wtk;
        }

        public Builder setWtk(WeakThresholdKey wtk) {
            this.wtk = wtk;
            return this;
        }
    }
}
