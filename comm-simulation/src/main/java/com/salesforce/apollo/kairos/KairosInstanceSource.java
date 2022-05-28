/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.kairos;

import java.time.Instant;
import java.time.InstantSource;

/**
 * Controls Kairos (simulation) time
 * 
 * @author hal.hildebrand
 *
 */
public class KairosInstanceSource implements InstantSource {
    private volatile Instant instant;

    public KairosInstanceSource(Instant instant) {
        this.instant = instant;
    }

    public void advance(Instant instant) {
        this.instant = instant;
    }

    @Override
    public Instant instant() {
        return instant;
    }
}
