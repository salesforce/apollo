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

import com.salesforce.apollo.slush.config.SlushParameters;
 

/**
 * Implements Slush, the first of the Avalanche protocols.
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class Slush<T> extends AbstractProtocol<SlushParameters, T> {
    private final static Logger log = LoggerFactory.getLogger(Slush.class);

    protected volatile int round;

    public Slush(Communications communications, SlushParameters parameters, Random entropy, T initialValue,
            Collection<T> enumeration, ScheduledExecutorService scheduler) {
        super(initialValue, enumeration, communications, parameters, entropy, scheduler);
        this.round = parameters.rounds;
    }

    @Override
    protected T decide(Map<T, Integer> responses) {
        log.trace("{} results: {}", communications.address(), responses);

        for (Entry<T, Integer> entry : responses.entrySet()) {
            if (entry.getValue() > threshold) {
                return entry.getKey();
            }
        }
        // No decision
        return null;
    }

    @Override
    protected boolean shouldContinue() {
        round--;
        int theRound = round;
        log.debug("{} round: {}", communications.address(), theRound);
        return theRound >= 1;
    }
}
