/**
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.concurrency.limits.limit;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;

import com.netflix.concurrency.limits.Limit;

public class TracingLimitDecorator implements Limit {
    private final Logger log;
    private final Limit  delegate;

    public static TracingLimitDecorator wrap(Limit delegate, Logger log) {
        return new TracingLimitDecorator(delegate, log);
    }

    public TracingLimitDecorator(Limit delegate, Logger log) {
        this.delegate = delegate;
        this.log = log;
    }

    @Override
    public int getLimit() {
        return delegate.getLimit();
    }

    @Override
    public void onSample(long startTime, long rtt, int inflight, boolean didDrop) {
        log.debug("maxInFlight={} minRtt={} ms", inflight, TimeUnit.NANOSECONDS.toMicros(rtt) / 1000.0);
        delegate.onSample(startTime, rtt, inflight, didDrop);
    }

    @Override
    public void notifyOnChange(Consumer<Integer> consumer) {
        delegate.notifyOnChange(consumer);
    }
}
