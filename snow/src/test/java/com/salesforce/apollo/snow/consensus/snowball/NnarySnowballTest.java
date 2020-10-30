/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball;

import static com.salesforce.apollo.snow.consensus.snowball.ConsensusTest.Blue;
import static com.salesforce.apollo.snow.consensus.snowball.ConsensusTest.Green;
import static com.salesforce.apollo.snow.consensus.snowball.ConsensusTest.Red;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class NnarySnowballTest {

    @Test
    public void testIt() {
        int betaVirtuous = 2;
        int betaRogue = 2;

        NnarySnowball sb = new NnarySnowball(betaVirtuous, betaRogue, Red);

        sb.add(Blue);
        sb.add(Green);

        ID pref = sb.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertFalse(sb.finalized(), "Finalized too early");

        sb.recordSuccessfulPoll(Blue);

        pref = sb.preference();
        assertEquals(Blue, pref, "Wrong preference");
        assertFalse(sb.finalized(), "Finalized too early");

        sb.recordSuccessfulPoll(Red);

        pref = sb.preference();
        assertEquals(Blue, pref, "Wrong preference");
        assertFalse(sb.finalized(), "Finalized too early");

        sb.recordSuccessfulPoll(Blue);

        pref = sb.preference();
        assertEquals(Blue, pref, "Wrong preference");
        assertTrue(sb.finalized(), "Did not finalize");
    }

    @Test
    public void virtuous() {
        int betaVirtuous = 1;
        int betaRogue = 2;

        NnarySnowball sb = new NnarySnowball(betaVirtuous, betaRogue, Red);

        ID pref = sb.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertFalse(sb.finalized(), "Finalized too early");

        sb.recordSuccessfulPoll(Red);

        pref = sb.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertTrue(sb.finalized(), "Did not finalize");

    }

    @Test
    public void unsucessfulPoll() {
        int betaVirtuous = 2;
        int betaRogue = 2;

        NnarySnowball sb = new NnarySnowball(betaVirtuous, betaRogue, Red);
        sb.add(Blue);

        ID pref = sb.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertFalse(sb.finalized(), "Finalized too early");

        sb.recordSuccessfulPoll(Blue);

        pref = sb.preference();
        assertEquals(Blue, pref, "Wrong preference");
        assertFalse(sb.finalized(), "Finalized too early");

        sb.recordUnsuccessfulPoll();

        sb.recordSuccessfulPoll(Blue);

        pref = sb.preference();
        assertEquals(Blue, pref, "Wrong preference");
        assertFalse(sb.finalized(), "Finalized too early");

        sb.recordSuccessfulPoll(Blue);

        pref = sb.preference();
        assertEquals(Blue, pref, "Wrong preference");
        assertTrue(sb.finalized(), "Did not finalize");

        for (int i = 0; i < 4; i++) {
            sb.recordSuccessfulPoll(Red);

            pref = sb.preference();
            assertEquals(Blue, pref, "Wrong preference");
            assertTrue(sb.finalized(), "Did not finalize");
        }
    }

    @Test
    public void differentSnowflakeColor() {
        int betaVirtuous = 2;
        int betaRogue = 2;

        NnarySnowball sb = new NnarySnowball(betaVirtuous, betaRogue, Red);
        sb.add(Blue);

        ID pref = sb.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertFalse(sb.finalized(), "Finalized too early");

        sb.recordSuccessfulPoll(Blue);

        pref = sb.preference();
        assertEquals(Blue, pref, "Wrong preference");

        sb.recordSuccessfulPoll(Red);

        pref = sb.preference();
        assertEquals(Blue, pref, "Wrong preference");
        
        pref = sb.sfPreference();
        assertEquals(Red, pref, "Wrong preference");
    }
}
