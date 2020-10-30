/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball;

import static com.salesforce.apollo.snow.consensus.snowball.ConsensusTest.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class NnarySnowflakeTest {

    @Test
    public void testIt() {
        int betaVirtuous = 2;
        int betaRogue = 2;

        NnarySnowflake sf = new NnarySnowflake(betaVirtuous, betaRogue, Red);
        sf.add(Blue);
        sf.add(Green);

        ID pref = sf.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertFalse(sf.finalized(), "Finalized too early");

        sf.recordSuccessfulPoll(Blue);

        pref = sf.preference();
        assertEquals(Blue, pref, "Wrong preference");
        assertFalse(sf.finalized(), "Finalized too early");

        sf.recordSuccessfulPoll(Red);

        pref = sf.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertFalse(sf.finalized(), "Finalized too early");

        sf.recordSuccessfulPoll(Red);

        pref = sf.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertTrue(sf.finalized(), "Did not finalize");

        sf.recordSuccessfulPoll(Blue);

        pref = sf.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertTrue(sf.finalized(), "Did not finalize");
    }

    @Test
    public void rogue() {
        int betaVirtuous = 2;
        int betaRogue = 2;

        NnarySnowflake sf = new NnarySnowflake(betaVirtuous, betaRogue, Red);
        assertFalse(sf.isRogue());

        sf.add(Red);
        assertFalse(sf.isRogue());

        sf.add(Blue);
        assertTrue(sf.isRogue());

        sf.add(Red);
        assertTrue(sf.isRogue());

        ID pref = sf.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertFalse(sf.finalized(), "Finalized too early");

        sf.recordSuccessfulPoll(Red);

        pref = sf.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertFalse(sf.finalized(), "Finalized too early");

        sf.recordSuccessfulPoll(Red);

        pref = sf.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertTrue(sf.finalized(), "Did not finalize");
    }

    @Test
    public void virtuous() {
        int betaVirtuous = 2;
        int betaRogue = 3;

        NnarySnowflake sf = new NnarySnowflake(betaVirtuous, betaRogue, Red);

        ID pref = sf.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertFalse(sf.finalized(), "Finalized too early");

        sf.recordSuccessfulPoll(Red);

        pref = sf.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertFalse(sf.finalized(), "Finalized too early");

        sf.recordSuccessfulPoll(Red);

        pref = sf.preference();
        assertEquals(Red, pref, "Wrong preference");
        assertTrue(sf.finalized(), "Did not finalize");

    }
}
