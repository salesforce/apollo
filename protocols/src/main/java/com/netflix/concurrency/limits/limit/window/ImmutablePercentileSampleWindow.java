/**
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.concurrency.limits.limit.window;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

class ImmutablePercentileSampleWindow implements SampleWindow {
    private final long minRtt;
    private final int maxInFlight;
    private final boolean didDrop;
    private final AtomicLongArray observedRtts;
    private final int sampleCount;
    private final double percentile;

    ImmutablePercentileSampleWindow(double percentile, int windowSize) {
        this.minRtt = Long.MAX_VALUE;
        this.maxInFlight = 0;
        this.didDrop = false;
        this.observedRtts = new AtomicLongArray(windowSize);
        this.sampleCount = 0;
        this.percentile = percentile;
    }

    private ImmutablePercentileSampleWindow(
            long minRtt,
            int maxInFlight,
            boolean didDrop,
            AtomicLongArray observedRtts,
            int sampleCount,
            double percentile
    ) {
        this.minRtt = minRtt;
        this.maxInFlight = maxInFlight;
        this.didDrop = didDrop;
        this.observedRtts = observedRtts;
        this.sampleCount = sampleCount;
        this.percentile = percentile;
    }

    @Override
    public ImmutablePercentileSampleWindow addSample(long rtt, int inflight, boolean didDrop) {
        if (sampleCount >= observedRtts.length()) {
            return this;
        }
        observedRtts.set(sampleCount, rtt);
        return new ImmutablePercentileSampleWindow(
                Math.min(minRtt, rtt),
                Math.max(inflight, this.maxInFlight),
                this.didDrop || didDrop,
                observedRtts,
                sampleCount + 1,
                percentile
        );
    }

    @Override
    public long getCandidateRttNanos() {
        return minRtt;
    }

    @Override
    public long getTrackedRttNanos() {
        if (sampleCount == 0) {
            return 0;
        }
        long[] copyOfObservedRtts = new long[sampleCount];
        for (int i = 0; i < sampleCount; i++) {
            copyOfObservedRtts[i] = observedRtts.get(i);
        }
        Arrays.sort(copyOfObservedRtts);

        int rttIndex = (int) Math.round(sampleCount * percentile);
        int zeroBasedRttIndex = rttIndex - 1;
        return copyOfObservedRtts[zeroBasedRttIndex];
    }

    @Override
    public int getMaxInFlight() {
        return maxInFlight;
    }

    @Override
    public int getSampleCount() {
        return sampleCount;
    }

    @Override
    public boolean didDrop() {
        return didDrop;
    }

    @Override
    public String toString() {
        return "ImmutablePercentileSampleWindow ["
                + "minRtt=" + TimeUnit.NANOSECONDS.toMicros(minRtt) / 1000.0
                + ", p" + percentile + " rtt=" + TimeUnit.NANOSECONDS.toMicros(getTrackedRttNanos()) / 1000.0
                + ", maxInFlight=" + maxInFlight
                + ", sampleCount=" + sampleCount
                + ", didDrop=" + didDrop + "]";
    }
}
