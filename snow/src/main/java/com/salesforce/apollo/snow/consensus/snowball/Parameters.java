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

    public final String         namespace;
    public final MetricRegistry metrics;
    public final int            k;
    public final int            alpha;
    public final int            betaVirtuous;
    public final int            betaRogue;
    public final int            concurrentRepolls;

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
