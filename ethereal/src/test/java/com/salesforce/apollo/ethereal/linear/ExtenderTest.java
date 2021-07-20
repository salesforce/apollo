/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.linear;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.ethereal.Config;
import com.salesforce.apollo.ethereal.DagFactory;
import com.salesforce.apollo.ethereal.DagFactory.DagAdder;
import com.salesforce.apollo.ethereal.DagReader;
import com.salesforce.apollo.ethereal.Unit;
import com.salesforce.apollo.ethereal.linear.CRPTest.RandomSourceMock;

/**
 * @author hal.hildebrand
 *
 */
public class ExtenderTest {

    static short crpFixedPrefix = 5;

    @Test
    public void emptyDagOnLevel0NextRoundReturnsNullNextRound() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/empty.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new RandomSourceMock();
        var cnf = Config.Builder.empty().setOrderStartLevel(0).setCrpFixedPrefix(crpFixedPrefix).build();
        var ordering = new Extender(d.dag(), rs, cnf);
        assertNull(ordering.nextRound());
    }

    @Test
    public void onlyDealingOnLevel0NextRoundReturnsNullNextRound() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/10/only_dealing.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new RandomSourceMock();
        var cnf = Config.Builder.empty().setOrderStartLevel(0).setCrpFixedPrefix(crpFixedPrefix).build();
        var ordering = new Extender(d.dag(), rs, cnf);
        assertNull(ordering.nextRound());
    }

    @Test
    public void onVeryRegularDagDecideUpTo8thLevel() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new RandomSourceMock();
        var cnf = Config.Builder.empty().setOrderStartLevel(0).setCrpFixedPrefix(crpFixedPrefix).build();
        var ordering = new Extender(d.dag(), rs, cnf);

        for (int level = 0; level < 8; level++) {
            assertNotNull(ordering.nextRound(), "failed at level:  " + level);
        }
        assertNull(ordering.nextRound());
    }

    @Test
    public void veryRegularDagTimingRounds() throws Exception {
        DagAdder d = null;
        try (FileInputStream fis = new FileInputStream(new File("src/test/resources/dags/4/regular.txt"))) {
            d = DagReader.readDag(fis, new DagFactory.TestDagFactory());
        }
        var rs = new RandomSourceMock();
        var cnf = Config.Builder.empty().setOrderStartLevel(0).setCrpFixedPrefix(crpFixedPrefix).build();
        var ordering = new Extender(d.dag(), rs, cnf);

        var timingRounds = new ArrayList<List<Unit>>();
        for (int level = 0; level < 8; level++) {
            TimingRound timingRound = ordering.nextRound();
            assertNotNull(timingRound, "failed at level:  " + level);
            var thisRound = timingRound.orderedUnits();
            assertNotNull(thisRound);
            timingRounds.add(thisRound);
        }
        assertNull(ordering.nextRound());

        // each level choose timing unit on this level
        for (int level = 0; level < 8; level++) {
            var tu = timingRounds.get(level).get(timingRounds.get(level).size() - 1);
            assertEquals(level, tu.level());
        }
        // should sort units in order consistent with the dag order
        var orderedUnits = new ArrayList<Unit>();
        for (int level = 0; level < 8; level++) {
            orderedUnits.addAll(timingRounds.get(level));
        }
        for (int i = 0; i < orderedUnits.size(); i++) {
            for (int j = i + 1; j < orderedUnits.size(); j++) {
                assertFalse(orderedUnits.get(i).above(orderedUnits.get(j)));
            }
        }
        // should on each level choose units that are below current timing unit but not
        // below previous timing units
        var timingUnits = new ArrayList<Unit>();
        for (int level = 0; level < 8; level++) {
            var tu = timingRounds.get(level).get(timingRounds.get(level).size() - 1);
            for (var u : timingRounds.get(level)) {
                for (var ptu : timingUnits) {
                    assertFalse(ptu.above(u));
                }
                assertTrue(tu.above(u));
            }
            timingUnits.add(tu);
        }
    }
}
