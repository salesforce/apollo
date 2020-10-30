/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowman.bootstrap;

import java.util.Set;

import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.engine.common.Bootstrapable;
import com.salesforce.apollo.snow.engine.common.Sender;
import com.salesforce.apollo.snow.engine.common.queue.Jobs;
import com.salesforce.apollo.snow.validators.Validator;
import com.salesforce.apollo.snow.validators.ValidatorSet;

/**
 * @author hal.hildebrand
 *
 */
public class Config extends com.salesforce.apollo.snow.engine.common.Config {
 
    public Config(ValidatorSet beacons, Bootstrapable bootstrappable, Context ctx, int sampleK, Sender sender,
            long alpha, Set<Validator> validator, long startupAlpha) {
        super(beacons, bootstrappable, ctx, sampleK, sender, alpha, validator, startupAlpha); 
    }
    
    protected Jobs blocked;
    protected ChainVM vm;
    

}
