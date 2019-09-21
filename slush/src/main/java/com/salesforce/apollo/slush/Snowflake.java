/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.slush;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.slush.config.SnowflakeParameters;

/**
 * Implements the BFT extension to Slush
 * 
 * @author hal.hildebrand
 * @since 218
 */
public class Snowflake<T> extends AbstractProtocol<SnowflakeParameters, T> {
    private final static Logger log = LoggerFactory.getLogger(Snowflake.class);

    protected volatile int count = 0;
    protected volatile boolean undecided = true;

    public Snowflake(Communications communications, SnowflakeParameters parameters, Random entropy, T initialValue,
            Collection<T> enumeration, ScheduledExecutorService scheduler) {
        super(initialValue, enumeration, communications, parameters, entropy, scheduler);
    }

    @Override
    protected T decide(Map<T, Integer> responses) {
        log.trace("{} results: {}", communications.address(), responses);

        for (Entry<T, Integer> entry : responses.entrySet()) {
            if (entry.getValue() > threshold) {
                if (getCurrent().equals(entry.getKey())) { // this is our current decision
                    if (++count > parameters.beta) { // and we're over our confidence level of seeing the same thing
                        undecided = false; // This is a wrap
                        return entry.getKey();
                    }
                } else {
                    count = 0; // Seen something different
                    return entry.getKey();
                }
            }
        }
        // No decision
        return null;
    }

    @Override
    protected boolean shouldContinue() {
        return undecided;
    }
}
