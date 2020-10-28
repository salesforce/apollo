/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.validators;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.salesforce.apollo.snow.ids.ShortID;
import com.salesforce.apollo.snow.sampler.WeightedWithoutReplacement;

/**
 * @author hal.hildebrand
 *
 */
public class ValidatorSet {
    private final Map<ShortID, Integer>      vdrMap = new HashMap<>();
    private long[]                           vdrWeights;
    private long                             totalWeight;
    private final WeightedWithoutReplacement sampler = null;

    // Set removes all the current validators and adds all the provided
    // validators to the set.
    public void set(Collection<Validator> validators) {
        
    }

    public Collection<Validator> sample(int sampleK) {
        // TODO Auto-generated method stub
        return null;
    }

    public Long getWeight(ShortID validatorID) {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return false;
    }
}
