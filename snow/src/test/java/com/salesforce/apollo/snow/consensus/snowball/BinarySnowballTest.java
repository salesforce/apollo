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
public class BinarySnowballTest {

    @Test
    public void acceptWeirdColor() {
        int blue = 0;
        int red = 1;

        int beta = 2;

        BinarySnowball sb = new BinarySnowball(beta, red);

        int pref = sb.preference();

        if (pref != red) {
            fail("Wrong pref, expected: " + red + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }

        sb.recordSuccessfulPoll(red);
        sb.recordUnsuccessfulPoll();

        pref = sb.preference();

        if (pref != red) {
            fail("Wrong pref, expected: " + red + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }

        sb.recordSuccessfulPoll(red);
        sb.recordUnsuccessfulPoll();

        pref = sb.preference();

        if (pref != red) {
            fail("Wrong pref, expected: " + red + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }

        sb.recordSuccessfulPoll(blue);

        pref = sb.preference();

        if (pref != red) {
            fail("Wrong pref, expected: " + red + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }

        sb.recordSuccessfulPoll(blue);

        pref = sb.preference();

        if (pref != blue) {
            fail("Wrong pref, expected: " + blue + " got " + pref);
        } else if (!sb.finalized()) {
            fail("Did not finalize");
        }
    }

    @Test
    public void lockColor() {
        int blue = 0;
        int red = 1;

        int beta = 2;

        BinarySnowball sb = new BinarySnowball(beta, red);

        sb.recordSuccessfulPoll(red);

        int pref = sb.preference();

        if (pref != red) {
            fail("Wrong pref, expected: " + red + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }

        sb.recordSuccessfulPoll(blue);

        pref = sb.preference();

        if (pref != red) {
            fail("Wrong pref, expected: " + red + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }
    }

    @Test
    public void recordUnsuccessfulPoll() {
        int blue = 0;
        int red = 1;

        int beta = 2;

        BinarySnowball sb = new BinarySnowball(beta, red);

        int pref = sb.preference();

        if (pref != red) {
            fail("Wrong pref, expected: " + red + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }

        sb.recordUnsuccessfulPoll();

        sb.recordSuccessfulPoll(blue);

        pref = sb.preference();

        if (pref != blue) {
            fail("Wrong pref, expected: " + blue + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }

        sb.recordSuccessfulPoll(blue);

        pref = sb.preference();

        if (pref != blue) {
            fail("Wrong pref, expected: " + blue + " got " + pref);
        } else if (!sb.finalized()) {
            fail("Did not finalize");
        }
    }

    @Test
    public void testIt() {
        int red = 0;
        int blue = 1;

        int beta = 2;
        BinarySnowball sb = new BinarySnowball(beta, red);

        int pref = sb.preference();
        if (pref != red) {
            fail("Wrong pref, expected: " + red + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }
        sb.recordSuccessfulPoll(blue);

        pref = sb.preference();
        if (pref != blue) {
            fail("Wrong pref, expected: " + blue + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }

        sb.recordSuccessfulPoll(red);

        pref = sb.preference();
        if (pref != blue) {
            fail("Wrong pref, expected: " + blue + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }

        sb.recordSuccessfulPoll(blue);

        pref = sb.preference();
        if (pref != blue) {
            fail("Wrong pref, expected: " + blue + " got " + pref);
        } else if (sb.finalized()) {
            fail("Finalized too early");
        }

        sb.recordSuccessfulPoll(blue);

        pref = sb.preference();
        if (pref != blue) {
            fail("Wrong pref, expected: " + blue + " got " + pref);
        } else if (!sb.finalized()) {
            fail("Did not finalize");
        }

    }
}
