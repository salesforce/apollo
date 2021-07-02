/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging.causal;

import java.time.Clock;
import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.membership.messaging.MessagingMetrics;
import com.salesforce.apollo.utils.bloomFilters.ClockValue;

public class Parameters {
    public static class Builder {
        private int                    bufferSize      = 500;
        private Comparator<ClockValue> comparator;
        private Context<Member>        context;
        private DigestAlgorithm        digestAlgorithm = DigestAlgorithm.DEFAULT;
        private Executor               executor        = ForkJoinPool.commonPool();
        private SigningMember          member;
        private MessagingMetrics       metrics;
        private Duration               tooOld          = Duration.ofMinutes(1);
        private java.time.Clock        wallclock       = Clock.systemUTC();

        public Parameters build() {
            return new Parameters(bufferSize, comparator, context, digestAlgorithm, executor, member, metrics, tooOld,
                    wallclock);
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public Comparator<ClockValue> getComparator() {
            return comparator;
        }

        public Context<Member> getContext() {
            return context;
        }

        public DigestAlgorithm getDigestAlgorithm() {
            return digestAlgorithm;
        }

        public SigningMember getMember() {
            return member;
        }

        public MessagingMetrics getMetrics() {
            return metrics;
        }

        public Duration getTooOld() {
            return tooOld;
        }

        public java.time.Clock getWallclock() {
            return wallclock;
        }

        public Parameters.Builder setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public Parameters.Builder setComparator(Comparator<ClockValue> comparator) {
            this.comparator = comparator;
            return this;
        }

        public Parameters.Builder setContext(Context<Member> context) {
            this.context = context;
            return this;
        }

        public Parameters.Builder setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
            return this;
        }

        public Parameters.Builder setMember(SigningMember member) {
            this.member = member;
            return this;
        }

        public Parameters.Builder setMetrics(MessagingMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public Parameters.Builder setTooOld(Duration tooOld) {
            this.tooOld = tooOld;
            return this;
        }

        public Parameters.Builder setWallclock(java.time.Clock wallclock) {
            this.wallclock = wallclock;
            return this;
        }
    }

    public static Parameters.Builder newBuilder() {
        return new Builder();
    }

    public final int                    bufferSize;
    public final Comparator<ClockValue> comparator;
    public final Context<Member>        context;
    public final DigestAlgorithm        digestAlgorithm;
    public final Executor               executor;
    public final SigningMember          member;
    public final MessagingMetrics       metrics;
    public final Duration               tooOld;
    public final java.time.Clock        wallclock;

    public Parameters(int bufferSize, Comparator<ClockValue> comparator, Context<Member> context,
            DigestAlgorithm digestAlgorithm, Executor executor, SigningMember member, MessagingMetrics metrics,
            Duration tooOld, Clock wallclock) {
        this.bufferSize = bufferSize;
        this.comparator = comparator;
        this.context = context;
        this.digestAlgorithm = digestAlgorithm;
        this.executor = executor;
        this.member = member;
        this.metrics = metrics;
        this.tooOld = tooOld;
        this.wallclock = wallclock;
    }
}
