/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.common;

import java.util.Set;

import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.validators.Validator;
import com.salesforce.apollo.snow.validators.ValidatorSet;

/**
 * @author hal.hildebrand
 *
 */
public class Config {
    protected final ValidatorSet   beacons;
    protected final Bootstrapable  bootstrapable;
    protected final Context        ctx;
    protected final int            sampleK;
    protected final Sender         sender;
    protected final long           alpha;
    protected final Set<Validator> validator;
    protected final long startupAlpha;

    public Config(ValidatorSet beacons, Bootstrapable bootstrappable, Context ctx, int sampleK, Sender sender,
            long alpha, Set<Validator> validator, long startupAlpha) {
        this.beacons = beacons;
        this.bootstrapable = bootstrappable;
        this.ctx = ctx;
        this.sampleK = sampleK;
        this.sender = sender;
        this.alpha = alpha;
        this.validator = validator;
        this.startupAlpha = startupAlpha;
    }

    public Context getContext() {
        return ctx;
    }

    public boolean isBootstrapped() {
        return ctx.isBootstrapped();
    }

}
