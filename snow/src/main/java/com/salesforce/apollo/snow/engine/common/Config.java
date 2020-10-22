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

/**
 * @author hal.hildebrand
 *
 */
abstract public class Config implements Engine {
    protected final Set<Validator> beacons;
    protected final Bootstrapable  bootstrappable;
    protected final Context        ctx;
    protected final int            sampleK;
    protected final Sender         sender;
    protected final long           startupAlpha;
    protected final Set<Validator> validator;

    public Config(Set<Validator> beacons, Bootstrapable bootstrappable, Context ctx, int sampleK, Sender sender,
            long startupAlpha, Set<Validator> validator) {
        this.beacons = beacons;
        this.bootstrappable = bootstrappable;
        this.ctx = ctx;
        this.sampleK = sampleK;
        this.sender = sender;
        this.startupAlpha = startupAlpha;
        this.validator = validator;
    }

    @Override
    public Context getContext() {
        return ctx;
    }

    @Override
    public boolean isBootstrapped() {
        return ctx.isBootstrapped();
    }

}
