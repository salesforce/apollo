/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.random.beacon;

import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.RandomSource.RandomSourceFactory;
import com.salesforce.apollo.ethereal.WeakThresholdKey;

/**
 * @author hal.hildebrand
 *
 */
public class Beacon implements RandomSourceFactory {
    private final Config config;

    public Beacon(Config config) {
        this.config = config;
    }

    public WeakThresholdKey getWTK(short creator) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] dealingData(int epoch) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RandomSource newRandomSource(Dag dag) {
        // TODO Auto-generated method stub
        return null;
    }
 

}
