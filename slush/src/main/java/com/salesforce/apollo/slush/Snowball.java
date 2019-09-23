/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.slush;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.slush.config.SnowflakeParameters;

/**
 * Adds confidence scoring to Snowflake
 * 
 * @author hal.hildebrand
 * @since 218
 */
public class Snowball<T> extends AbstractProtocol<SnowflakeParameters, T> {
    private final static Logger log = LoggerFactory.getLogger(Snowball.class);

    private final Map<T, Integer> confidence;
    private volatile int count = 0;
    private volatile T lastValue;
    private volatile boolean undecided = true;

    public Snowball(Communications communications, SnowflakeParameters parameters, Random entropy,
            Collection<T> enumeration, ScheduledExecutorService scheduler, T initialValue) {
        super(initialValue, enumeration, communications, parameters, entropy, scheduler);
        lastValue = initialValue;
        confidence = new HashMap<>();
        for (T element : enumeration) {
            confidence.put(element, 0);
        }
    }

    @Override
    protected T decide(Map<T, Integer> responses) {
        log.trace("{} results: {}", communications.address(), responses);

        for (Entry<T, Integer> entry : responses.entrySet()) {
            if (entry.getValue() > threshold) {
                int choiceConf = increaseConfidence(entry.getKey());
                if (getCurrent().equals(entry.getKey()) || choiceConf > confidence.get(getCurrent())) {
                    setCurrent(entry.getKey());

                    if (lastValue != entry.getKey()) {
                        lastValue = entry.getKey();
                        count = 0;
                    } else {
                        if (++count > parameters.beta) {
                            undecided = false;
                        }
                    }
                }
                return entry.getKey();
            }
        }
        // No decision
        return null;
    }

    @Override
    protected boolean shouldContinue() {
        return undecided;
    }

    private int increaseConfidence(T choice) {
        int incremented = confidence.get(choice) + 1;
        confidence.put(choice, incremented);
        return incremented;
    }
}
