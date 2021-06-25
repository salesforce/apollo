/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.utils.IBF.Decode;

/**
 * @author hal.hildebrand
 *
 */
public class IntIBFTest {

    private Random       r;
    private IBF<Integer> ibf;

    @BeforeEach
    public void before() {
        r = new Random(0x1638);
        ibf = new IBF.IntIBF(2000, 19932631, 3);
    }

    @Test
    public void add() {
        List<Integer> inserted = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            int val = r.nextInt(9999);
            inserted.add(val);
            ibf.add(val);
        }
        for (int tst : inserted) {
            assert (ibf.contains(tst));
        }
    }

    @Test
    public void pure() throws Exception {
        int val = r.nextInt(9999);
        ibf.add(val);
        int count = 0;
        for (int cell = 0; cell < ibf.cells(); cell++) {
            if (ibf.isPure(cell))
                count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void decode() throws Exception {

        int seed1 = r.nextInt();
        IBF<Integer> b1 = new IBF.IntIBF(100, seed1, 3);
        IBF<Integer> b2 = new IBF.IntIBF(100, seed1, 3);

        for (int i = 0; i < 50000; i++) {
            int val = r.nextInt(99999999);
            b1.add(val);
            b2.add(val);
        }

        int b1sb2[] = new int[10];
        int b2sb1[] = new int[20];

        for (int i = 0; i < b1sb2.length; i++) {
            int val = r.nextInt(99999999);
            b1.add(val);
            b1sb2[i] = val;
        }
        for (int i = 0; i < b2sb1.length; i++) {
            int val = r.nextInt(99999999);
            b2.add(val);
            b2sb1[i] = val;
        }

        Arrays.sort(b1sb2);
        Arrays.sort(b2sb1);

        IBF<Integer> res = b1.subtract(b2);
        Decode<Integer> diff = b1.decode(res);
        assertNotNull(diff);
        assertEquals(b1sb2.length, diff.added().size(), "invalid added");
        assertEquals(b2sb1.length, diff.missing().size(), "invalid missing");

        Collections.sort(diff.added());
        Collections.sort(diff.missing());

        System.out.println("===========");

        for (int i = 0; i < diff.added().size(); i++) {
            System.out.println(b1sb2[i] + "," + diff.added().get(i));
        }
        System.out.println("..........");
        for (int i = 0; i < diff.missing().size(); i++) {
            System.out.println(b2sb1[i] + "," + diff.missing().get(i));
        }

    }
}
