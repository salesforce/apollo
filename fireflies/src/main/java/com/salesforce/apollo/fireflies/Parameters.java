/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.time.Duration;

/**
 * @author hal.hildebrand
 *
 */
public record Parameters(int joinRetries, int minimumBiffCardinality, int rebuttalTimeout, int viewChangeRounds,
                         int finalizeViewRounds, double fpr, int maximumTxfr, Duration retryDelay, int maxPending,
                         Duration seedingTimeout, GorgoneionParameters gorgoneion) {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        /**
         * Number of TTL rounds to wait before finalizing a view change
         */
        private int                          finalizeViewRounds     = 3;
        /**
         * False positive rate for bloom filter state replication (high fpr is good)
         */
        private double                       fpr                    = 0.0125;
        /**
         * Identity attestation/verification
         */
        private GorgoneionParameters.Builder gorgoneion             = GorgoneionParameters.newBuilder();
        /**
         * Number of retries when joining until giving up
         */
        private int                          joinRetries            = 500;
        /**
         * Maximum number of elements to transfer per type per update
         */
        private int                          maximumTxfr            = 10;
        /**
         * Maximum pending joins
         * 
         */
        private int                          maxPending             = 15;
        /**
         * Minimum cardinality for bloom filters
         */
        private int                          minimumBiffCardinality = 128;
        /**
         * Number of TTL rounds an accussed has to rebut the accusation
         */
        private int                          rebuttalTimeout        = 2;
        /**
         * Max duration to delay retrying join operations
         */
        private Duration                     retryDelay             = Duration.ofMillis(200);
        /**
         * Timeout for contacting seed gateways during seeding and join operations
         */
        private Duration                     seedingTimout          = Duration.ofSeconds(5);

        /**
         * Minimum number of rounds to check for view change
         */
        private int viewChangeRounds = 7;

        public Parameters build() {
            return new Parameters(joinRetries, minimumBiffCardinality, rebuttalTimeout, viewChangeRounds,
                                  finalizeViewRounds, fpr, maximumTxfr, retryDelay, maxPending, seedingTimout,
                                  gorgoneion.build());
        }

        public int getFinalizeViewRounds() {
            return finalizeViewRounds;
        }

        public double getFpr() {
            return fpr;
        }

        public GorgoneionParameters.Builder getGorgoneion() {
            return gorgoneion;
        }

        public int getJoinRetries() {
            return joinRetries;
        }

        public int getMaximumTxfr() {
            return maximumTxfr;
        }

        public int getMaxPending() {
            return maxPending;
        }

        public int getMinimumBiffCardinality() {
            return minimumBiffCardinality;
        }

        public int getRebuttalTimeout() {
            return rebuttalTimeout;
        }

        public Duration getRetryDelay() {
            return retryDelay;
        }

        public Duration getSeedingTimout() {
            return seedingTimout;
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

        public Builder setGorgoneion(GorgoneionParameters.Builder gorgoneion) {
            this.gorgoneion = gorgoneion;
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

        public Builder setMaxPending(int maxPending) {
            this.maxPending = maxPending;
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

        public Builder setRetryDelay(Duration retryDelay) {
            this.retryDelay = retryDelay;
            return this;
        }

        public Builder setSeedingTimout(Duration seedingTimout) {
            this.seedingTimout = seedingTimout;
            return this;
        }

        public Builder setViewChangeRounds(int viewChangeRounds) {
            this.viewChangeRounds = viewChangeRounds;
            return this;
        }
    }

}
