/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

/**
 * @author hal.hildebrand
 *
 */
public record Parameters(int joinRetries, int minimumBiffCardinality, int rebuttalTimeout, int viewChangeRounds,
                         int finalizeViewRounds, double fpr, int maximumTxfr) {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        /**
         * Number of TTL rounds to wait before finalizing a view change
         */
        private int    finalizeViewRounds     = 3;
        /**
         * False positive rate for bloom filter state replication (high fpr is good)
         */
        private double fpr                    = 0.0125;
        /**
         * Number of retries when joining until giving up
         */
        private int    joinRetries            = 3;
        /**
         * Maximum number of elements to transfer per type per update
         */
        private int    maximumTxfr            = 100;
        /**
         * Minimum cardinality for bloom filters
         */
        private int    minimumBiffCardinality = 100;
        /**
         * Number of TTL rounds an accussed has to rebut the accusation
         */
        private int    rebuttalTimeout        = 3;
        /**
         * Minimum number of rounds to check for view change
         */
        private int    viewChangeRounds       = 7;

        public Parameters build() {
            return new Parameters(joinRetries, minimumBiffCardinality, rebuttalTimeout, viewChangeRounds,
                                  finalizeViewRounds, fpr, maximumTxfr);
        }

        public int getFinalizeViewRounds() {
            return finalizeViewRounds;
        }

        public double getFpr() {
            return fpr;
        }

        public int getJoinRetries() {
            return joinRetries;
        }

        public int getMaximumTxfr() {
            return maximumTxfr;
        }

        public int getMinimumBiffCardinality() {
            return minimumBiffCardinality;
        }

        public int getRebuttalTimeout() {
            return rebuttalTimeout;
        }

        public int getViewChangeRounds() {
            return viewChangeRounds;
        }

        public Builder setFinalizeViewRounds(int finalizeViewRounds) {
            this.finalizeViewRounds = finalizeViewRounds;
            return this;
        }

        public Builder setFpr(double fpr) {
            this.fpr = fpr;
            return this;
        }

        public Builder setJoinRetries(int joinRetries) {
            this.joinRetries = joinRetries;
            return this;
        }

        public Builder setMaximumTxfr(int maximumTxfr) {
            this.maximumTxfr = maximumTxfr;
            return this;
        }

        public Builder setMinimumBiffCardinality(int minimumBiffCardinality) {
            this.minimumBiffCardinality = minimumBiffCardinality;
            return this;
        }

        public Builder setRebuttalTimeout(int rebuttalTimeout) {
            this.rebuttalTimeout = rebuttalTimeout;
            return this;
        }

        public Builder setViewChangeRounds(int viewChangeRounds) {
            this.viewChangeRounds = viewChangeRounds;
            return this;
        }
    }

}
