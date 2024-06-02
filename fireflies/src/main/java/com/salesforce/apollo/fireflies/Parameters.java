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
 */
public record Parameters(int joinRetries, int minimumBiffCardinality, int rebuttalTimeout, int viewChangeRounds,
                         int finalizeViewRounds, double fpr, int maximumTxfr, Duration retryDelay, int maxPending,
                         Duration seedingTimeout, int validationRetries, int crowns, Duration populateDuration) {

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        /**
         * Number of crowns for the view's hexbloom
         */
        private int      crowns                 = 2;
        /**
         * Number of TTL rounds to wait before finalizing a view change
         */
        private int      finalizeViewRounds     = 3;
        /**
         * False positive rate for bloom filter state replication (high fpr is good)
         */
        private double   fpr                    = 0.00125;
        /**
         * Number of retries when joining until giving up
         */
        private int      joinRetries            = 500;
        /**
         * Maximum number of elements to transfer per type per update
         */
        private int      maximumTxfr            = 1024;
        /**
         * Maximum pending joins
         */
        private int      maxPending             = 200;
        /**
         * Minimum cardinality for bloom filters
         */
        private int      minimumBiffCardinality = 1025;
        /**
         * Number of TTL rounds an accused has to rebut the accusation
         */
        private int      rebuttalTimeout        = 2;
        /**
         * Max duration to delay retrying join operations
         */
        private Duration retryDelay             = Duration.ofMillis(200);
        /**
         * Timeout for contacting seed gateways during seeding and join operations
         */
        private Duration seedingTimout          = Duration.ofSeconds(15);
        /**
         * Max number of times to attempt validation when joining a view
         */
        private int      validationRetries      = 3;
        /**
         * Minimum number of rounds to check for view change
         */
        private int      viewChangeRounds       = 7;
        private Duration populateDuration       = Duration.ofMillis(20);

        public Parameters build() {
            return new Parameters(joinRetries, minimumBiffCardinality, rebuttalTimeout, viewChangeRounds,
                                  finalizeViewRounds, fpr, maximumTxfr, retryDelay, maxPending, seedingTimout,
                                  validationRetries, crowns, populateDuration);
        }

        public int getCrowns() {
            return crowns;
        }

        public Builder setCrowns(int crowns) {
            this.crowns = crowns;
            return this;
        }

        public int getFinalizeViewRounds() {
            return finalizeViewRounds;
        }

        public Builder setFinalizeViewRounds(int finalizeViewRounds) {
            this.finalizeViewRounds = finalizeViewRounds;
            return this;
        }

        public double getFpr() {
            return fpr;
        }

        public Builder setFpr(double fpr) {
            this.fpr = fpr;
            return this;
        }

        public int getJoinRetries() {
            return joinRetries;
        }

        public Builder setJoinRetries(int joinRetries) {
            this.joinRetries = joinRetries;
            return this;
        }

        public int getMaxPending() {
            return maxPending;
        }

        public Builder setMaxPending(int maxPending) {
            this.maxPending = maxPending;
            return this;
        }

        public int getMaximumTxfr() {
            return maximumTxfr;
        }

        public Builder setMaximumTxfr(int maximumTxfr) {
            this.maximumTxfr = maximumTxfr;
            return this;
        }

        public int getMinimumBiffCardinality() {
            return minimumBiffCardinality;
        }

        public Builder setMinimumBiffCardinality(int minimumBiffCardinality) {
            this.minimumBiffCardinality = minimumBiffCardinality;
            return this;
        }

        public Duration getPopulateDuration() {
            return populateDuration;
        }

        public Builder setPopulateDuration(Duration populateDuration) {
            this.populateDuration = populateDuration;
            return this;
        }

        public int getRebuttalTimeout() {
            return rebuttalTimeout;
        }

        public Builder setRebuttalTimeout(int rebuttalTimeout) {
            this.rebuttalTimeout = rebuttalTimeout;
            return this;
        }

        public Duration getRetryDelay() {
            return retryDelay;
        }

        public Builder setRetryDelay(Duration retryDelay) {
            this.retryDelay = retryDelay;
            return this;
        }

        public Duration getSeedingTimout() {
            return seedingTimout;
        }

        public Builder setSeedingTimout(Duration seedingTimout) {
            this.seedingTimout = seedingTimout;
            return this;
        }

        public int getValidationRetries() {
            return validationRetries;
        }

        public Builder setValidationRetries(int validationRetries) {
            this.validationRetries = validationRetries;
            return this;
        }

        public int getViewChangeRounds() {
            return viewChangeRounds;
        }

        public Builder setViewChangeRounds(int viewChangeRounds) {
            this.viewChangeRounds = viewChangeRounds;
            return this;
        }
    }

}
