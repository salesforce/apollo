/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowball;

import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

/**
 * @author hal.hildebrand
 *
 */
public class BinarySnowflakeTest {

    @Test
    public void testIt() {
        int blue = 0;
        int red = 1;
        
        int beta = 2;
        
        BinarySnowflake sf = new BinarySnowflake(beta, red); 

        int pref = sf.preference();

        if (pref != red) {
            fail("Wrong pref, expected: " + red + " got " + pref);
        } else if (sf.finalized()) {
            fail("Finalized too early");
        }
        
        sf.recordSuccessfulPoll(blue); 

        pref = sf.preference();

        if (pref != blue) {
            fail("Wrong pref, expected: " + blue + " got " + pref);
        } else if (sf.finalized()) {
            fail("Finalized too early");
        }
        
        sf.recordSuccessfulPoll(red);
        
        pref = sf.preference();

        if (pref != red) {
            fail("Wrong pref, expected: " + red + " got " + pref);
        } else if (sf.finalized()) {
            fail("Finalized too early");
        }
        
        sf.recordSuccessfulPoll(blue); 

        pref = sf.preference();

        if (pref != blue) {
            fail("Wrong pref, expected: " + blue + " got " + pref);
        } else if (sf.finalized()) {
            fail("Finalized too early");
        }
        
        sf.recordSuccessfulPoll(blue); 

        pref = sf.preference();

        if (pref != blue) {
            fail("Wrong pref, expected: " + blue + " got " + pref);
        } else if (!sf.finalized()) {
            fail("Did not finalize");
        }
    }
}
