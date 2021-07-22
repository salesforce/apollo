/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal;

/**
 * RandomSource represents a source of randomness needed to run the protocol. It
 * specifies what kind of data should be included in units, and can use this
 * data to generate random bytes.
 * 
 * @author hal.hildebrand
 *
 */
public interface RandomSource {
    interface RandomSourceFactory {
        /**
         * DealingData returns random source data that should be included in the dealing
         * unit for the given epoch.
         */
        byte[] dealingData(int epoch);

        /** NewRandomSource produces a randomness source for the provided dag. */
        RandomSource newRandomSource(Dag dag);
    }

    /**
     * DataToInclude returns data which should be included in a unit based on its
     * level and parents.
     */
    byte[] dataToInclude(Unit[] parents, int level);

    /** RandomBytes returns random bytes for a given process and level. */
    byte[] randomBytes(short process, int level);
}
