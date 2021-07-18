/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.random.coin;

import java.util.List;

import com.salesforce.apollo.ethereal.Dag;
import com.salesforce.apollo.ethereal.RandomSource;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.WeakThresholdKey;

/**
 * @author hal.hildebrand
 *
 */
public class Coin implements RandomSource {
    
    public static RandomSourceFactory newFactory(short pid, WeakThresholdKey wtk) {
        return new RandomSourceFactory() {
            
            @Override
            public RandomSource newRandomSource(Dag dag) {
                // TODO Auto-generated method stub
                return null;
            }
            
            @Override
            public byte[] dealingData(int epoch) {
                // TODO Auto-generated method stub
                return null;
            }
        };
    }

    @Override
    public byte[] dataToInclude(List<Unit> parents, int level) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] randomBytes(short process, int level) {
        // TODO Auto-generated method stub
        return null;
    }

}
