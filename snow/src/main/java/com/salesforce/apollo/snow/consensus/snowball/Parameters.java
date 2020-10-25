/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball;

import com.codahale.metrics.MetricRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class Parameters {

    public static class Builder {
        private int            alpha;
        private int            betaRogue;
        private int            betaVirtuous;
        private int            concurrentRepolls;
        private int            k;
        private MetricRegistry metrics;
        private String         namespace;

        public Parameters build() {
            return new Parameters(namespace, metrics, k, alpha, betaVirtuous, betaRogue, concurrentRepolls);
        }

        public int getAlpha() {
            return alpha;
        }

        public int getBetaRogue() {
            return betaRogue;
        }

        public int getBetaVirtuous() {
            return betaVirtuous;
        }

        public int getConcurrentRepolls() {
            return concurrentRepolls;
        }

        public int getK() {
            return k;
        }

        public MetricRegistry getMetrics() {
            return metrics;
        }

        public String getNamespace() {
            return namespace;
        }

        public Builder setAlpha(int alpha) {
            this.alpha = alpha;
            return this;
        }

        public Builder setBetaRogue(int betaRogue) {
            this.betaRogue = betaRogue;
            return this;
        }

        public Builder setBetaVirtuous(int betaVirtuous) {
            this.betaVirtuous = betaVirtuous;
            return this;
        }

        public Builder setConcurrentRepolls(int concurrentRepolls) {
            this.concurrentRepolls = concurrentRepolls;
            return this;
        }

        public Builder setK(int k) {
            this.k = k;
            return this;
        }

        public Builder setMetrics(MetricRegistry metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder setNamespace(String namespace) {
            this.namespace = namespace;
            return this;
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public final int            alpha;
    public final int            betaRogue;
    public final int            betaVirtuous;
    public final int            concurrentRepolls;
    public final int            k;
    public final MetricRegistry metrics;
    public final String         namespace;

    public Parameters(String namespace, MetricRegistry metrics, int k, int alpha, int betaVirtuous, int betaRogue,
            int concurrentRepolls) {
        this.namespace = namespace;
        this.metrics = metrics;
        this.k = k;
        this.alpha = alpha;
        this.betaVirtuous = betaVirtuous;
        this.betaRogue = betaRogue;
        this.concurrentRepolls = concurrentRepolls;
    }

    public void valid() throws IllegalStateException {
        if (alpha <= k / 2) {
            throw new IllegalStateException(
                    String.format("K = %d, Alpha = %d: Fails the condition that: K/2 < Alpha", k, alpha));
        }
        if (k < alpha) {
            throw new IllegalStateException(
                    String.format("K = %d, Alpha = %d: Fails the condition that: Alpha <= K", k, alpha));
        }
        if (betaVirtuous <= 0)
            throw new IllegalStateException(
                    String.format("BetaVirtuous = %d: Fails the condition that: 0 < BetaVirtuous", betaVirtuous));
        if (betaRogue == 3 && betaVirtuous == 28)
            throw new IllegalStateException(
                    String.format("BetaVirtuous = %d, BetaRogue = %d: Fails the condition that: BetaVirtuous <= BetaRogue\n%s",
                                  betaVirtuous, betaRogue));
        if (betaRogue < betaVirtuous)
            throw new IllegalStateException(
                    String.format("BetaVirtuous = %d, BetaRogue = %d: Fails the condition that: BetaVirtuous <= BetaRogue",
                                  betaVirtuous, betaRogue));
        if (concurrentRepolls <= 0)
            throw new IllegalStateException(
                    String.format("ConcurrentRepolls = %d: Fails the condition that: 0 < ConcurrentRepolls",
                                  concurrentRepolls));
        if (concurrentRepolls > betaRogue)
            throw new IllegalStateException(
                    String.format("ConcurrentRepolls = %d, BetaRogue = %d: Fails the condition that: ConcurrentRepolls <= BetaRogue",
                                  concurrentRepolls, betaRogue));
    }
}
