/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.function.Supplier;

import com.codahale.metrics.Gauge;
import com.netflix.concurrency.limits.MetricRegistry;

/**
 * @author hal.hildebrand
 *
 */
public class LimitsRegistry implements MetricRegistry {

    private final com.codahale.metrics.MetricRegistry registry;
    private final String                              prefix;

    public LimitsRegistry(String prefix, com.codahale.metrics.MetricRegistry registry) {
        this.prefix = prefix;
        this.registry = registry;
    }

    @Override
    public SampleListener distribution(String id, String... tagNameValuePairs) {
        var histogram = registry.histogram(name(name(prefix, id), tagNameValuePairs));
        return new SampleListener() {
            @Override
            public void addSample(Number value) {
                histogram.update(value.longValue());
            }
        };
    }

    @Override
    public void gauge(String id, Supplier<Number> supplier, String... tagNameValuePairs) {
        registry.gauge(name(name(prefix, id), tagNameValuePairs), () -> new Gauge<Number>() {
            @Override
            public Number getValue() {
                return supplier.get();
            }
        });
    }

    @Override
    public Counter counter(String id, String... tagNameValuePairs) {
        var counter = registry.counter(name(name(prefix, id), tagNameValuePairs));
        return new Counter() {
            @Override
            public void increment() {
                counter.inc();
            }
        };
    }
}
