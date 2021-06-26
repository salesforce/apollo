/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.membership.Context;

/**
 * Parameters defining the operation of Fireflies
 *
 * @author hal.hildebrand
 * @since 220
 */
public class FirefliesParameters {
    public static class Builder {
        public int                   cardinality;
        public double                probabilityByzantine = 0.10;
        private CertificateValidator certificateValidator;
        private double               falsePositiveRate    = 0.125;
        private DigestAlgorithm      hashAlgorithm        = DigestAlgorithm.DEFAULT;
        private SignatureAlgorithm   signatureAlgorithm   = SignatureAlgorithm.DEFAULT;

        public FirefliesParameters build() {
            return new FirefliesParameters(cardinality, signatureAlgorithm, falsePositiveRate, hashAlgorithm,
                    probabilityByzantine, certificateValidator);
        }

        public int getCardinality() {
            return cardinality;
        }

        public CertificateValidator getCertificateValidator() {
            return certificateValidator;
        }

        public double getFalsePositiveRate() {
            return falsePositiveRate;
        }

        public DigestAlgorithm getHashAlgorithm() {
            return hashAlgorithm;
        }

        public double getProbabilityByzantine() {
            return probabilityByzantine;
        }

        public SignatureAlgorithm getSignatureAlgorithm() {
            return signatureAlgorithm;
        }

        public Builder setCardinality(int cardinality) {
            this.cardinality = cardinality;
            return this;
        }

        public Builder setCertificateValidator(CertificateValidator certificateValidator) {
            this.certificateValidator = certificateValidator;
            return this;
        }

        public Builder setFalsePositiveRate(double falsePositiveRate) {
            this.falsePositiveRate = falsePositiveRate;
            return this;
        }

        public Builder setHashAlgorithm(DigestAlgorithm hashAlgorithm) {
            this.hashAlgorithm = hashAlgorithm;
            return this;
        }

        public Builder setProbabilityByzantine(double probabilityByzantine) {
            this.probabilityByzantine = probabilityByzantine;
            return this;
        }

        public Builder setSignatureAlgorithm(SignatureAlgorithm signatureAlgorithm) {
            this.signatureAlgorithm = signatureAlgorithm;
            return this;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * The maximum cardinality of the fireflies group
     */
    public final int cardinality;

    public final CertificateValidator certificateValidator;

    /**
     * The false positive rate for the bloomfilters used for the antientropy
     * protocol
     */
    public final double             falsePositiveRate;
    /**
     * The algorithm used for secure hashes
     */
    public final DigestAlgorithm    hashAlgorithm;
    /**
     * The probability of a member being subversive
     */
    public final double             probabilityByzantine;
    /**
     * The number of rings based on the parameters required to resist failure in
     * gossip and byzantine members.
     */
    public final int                rings;
    /**
     * The JCE algorithm name used for signatures
     */
    public final SignatureAlgorithm signatureAlgorithm;

    /**
     * The number of rings tolerated either by a failure or through byzantine
     * subversion
     */
    public final int toleranceLevel;

    public FirefliesParameters(int cardinality, SignatureAlgorithm signatureAlgorithm, double probabilityByzantine,
            DigestAlgorithm hashAlgorithm, double falsePositiveRate, CertificateValidator certificateValidator) {
        this.cardinality = cardinality;
        this.signatureAlgorithm = signatureAlgorithm;
        this.hashAlgorithm = hashAlgorithm;
        this.falsePositiveRate = falsePositiveRate;
        this.probabilityByzantine = probabilityByzantine;
        toleranceLevel = Context.minMajority(probabilityByzantine, cardinality);
        rings = toleranceLevel * 2 + 1;
        this.certificateValidator = certificateValidator;
    }

    @Override
    public String toString() {
        return "cardinality=" + cardinality + ", hashAlgorithm=" + hashAlgorithm + ", probabilityByzantine="
                + probabilityByzantine + ", rings=" + rings + ", signatureAlgorithm=" + signatureAlgorithm
                + ", toleranceLevel=" + toleranceLevel;
    }
}
