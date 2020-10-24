/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * @author hal.hildebrand
 *
 */
public class UnarySnowflakeTest {

    static void testState(UnarySnowflake sf, int confidence, boolean finalized) {
        assertEquals(confidence, sf.confidence());
        assertEquals(finalized, sf.finalized());
    }

    @Test
    public void testIt() {
        int beta = 2;

        UnarySnowflake sf = new UnarySnowflake(beta);

        sf.recordSuccessfulPoll();
        testState(sf, 1, false);

        sf.recordUnsuccessfulPoll();
        testState(sf, 0, false);

        sf.recordSuccessfulPoll();
        testState(sf, 1, false);

        UnarySnowflake sfClone = sf.clone();
        testState(sfClone, 1, false);

        BinarySnowflake binarySnowflake = sf.extend(beta, 0);

        binarySnowflake.recordUnsuccessfulPoll();
        binarySnowflake.recordSuccessfulPoll(1);

        assertFalse(binarySnowflake.finalized());

        binarySnowflake.recordSuccessfulPoll(1);

        assertEquals(1, binarySnowflake.preference());
        assertTrue(binarySnowflake.finalized());

        sf.recordSuccessfulPoll();
        testState(sf, 2, true);

        sf.recordUnsuccessfulPoll();
        testState(sf, 0, true);

        sf.recordSuccessfulPoll();
        testState(sf, 1, true);

    }
}
