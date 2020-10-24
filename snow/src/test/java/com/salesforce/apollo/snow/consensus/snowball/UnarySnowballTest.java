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
public class UnarySnowballTest {

    static void testState(UnarySnowball sb, int succPolls, int confidence, boolean finalized) {
        assertEquals(succPolls, sb.getNumSuccessfulPolls());
        assertEquals(confidence, sb.confidence());
        assertEquals(finalized, sb.finalized());
    }

    @Test
    public void testIt() {
        int beta = 2;

        UnarySnowball sb = new UnarySnowball(beta);

        sb.recordSuccessfulPoll();
        testState(sb, 1, 1, false);

        sb.recordUnsuccessfulPoll();
        testState(sb, 1, 0, false);

        sb.recordSuccessfulPoll();
        testState(sb, 2, 1, false);

        UnarySnowball sbClone = sb.clone();
        testState(sbClone, 2, 1, false);

        BinarySnowball binarySnowball = sbClone.extend(beta, 0);

        binarySnowball.recordUnsuccessfulPoll();
        for (int i = 0; i < 3; i++) {
            assertEquals(0, binarySnowball.preference());
            assertFalse(binarySnowball.finalized());
            binarySnowball.recordSuccessfulPoll(1);
            binarySnowball.recordUnsuccessfulPoll();
        }
        
        assertEquals(1, binarySnowball.preference());
        assertFalse(binarySnowball.finalized());

        binarySnowball.recordSuccessfulPoll(1);
        assertEquals(1, binarySnowball.preference());
        assertFalse(binarySnowball.finalized());

        binarySnowball.recordSuccessfulPoll(1);
        assertEquals(1, binarySnowball.preference());
        assertTrue(binarySnowball.finalized());
    }
}
