/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import com.salesforce.apollo.crypto.DigestAlgorithm;
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
        private DigestAlgorithm      hashAlgorithm        = DigestAlgorithm.DEFAULT;

        public FirefliesParameters build() {
            return new FirefliesParameters(cardinality, hashAlgorithm, probabilityByzantine, certificateValidator);
        }

        public int getCardinality() {
            return cardinality;
        }

        public CertificateValidator getCertificateValidator() {
            return certificateValidator;
        }

        public DigestAlgorithm getHashAlgorithm() {
            return hashAlgorithm;
        }

        public double getProbabilityByzantine() {
            return probabilityByzantine;
        }

        public Builder setCardinality(int cardinality) {
            this.cardinality = cardinality;
            return this;
        }

        public Builder setCertificateValidator(CertificateValidator certificateValidator) {
            this.certificateValidator = certificateValidator;
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
     * The algorithm used for secure hashes
     */
    public final DigestAlgorithm      hashAlgorithm;
    /**
     * The probability of a member being subversive
     */
    public final double               probabilityByzantine;
    /**
     * The number of rings based on the parameters required to resist failure in
     * gossip and byzantine members.
     */
    public final int                  rings;

    /**
     * The number of rings tolerated either by a failure or through byzantine
     * subversion
     */
    public final int toleranceLevel;

    public FirefliesParameters(int cardinality, DigestAlgorithm hashAlgorithm, double probabilityByzantine,
                               CertificateValidator certificateValidator) {
        this.cardinality = cardinality;
        this.hashAlgorithm = hashAlgorithm;
        this.probabilityByzantine = probabilityByzantine;
        toleranceLevel = Context.minMajority(probabilityByzantine, cardinality);
        rings = toleranceLevel * 2 + 1;
        this.certificateValidator = certificateValidator;
    }

    @Override
    public String toString() {
        return "cardinality=" + cardinality + ", hashAlgorithm=" + hashAlgorithm + ", probabilityByzantine="
        + probabilityByzantine + ", rings=" + rings + ", toleranceLevel=" + toleranceLevel;
    }
}
