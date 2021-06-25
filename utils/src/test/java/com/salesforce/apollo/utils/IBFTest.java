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

/**
 * @author hal.hildebrand
 *
 */
public class IBFTest {

    private Random       r;
    private IBF<Integer> ibf;

    @BeforeEach
    public void before() {
        r = new Random(0x1638);
        ibf = new IBF.IntIBF(2000, r.nextInt(), 3);
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
        Pair<List<Integer>, List<Integer>> diff = b1.decode(res);
        assertNotNull(diff);
        assertEquals(diff.a.size(), b1sb2.length, "error b1sb2");
        assertEquals(diff.b.size(), b2sb1.length, "error b2sb1");

        Collections.sort(diff.a);
        Collections.sort(diff.b);

        System.out.println("===========");

        for (int i = 0; i < diff.a.size(); i++) {
            System.out.println(b1sb2[i] + "," + diff.a.get(i));
        }
        System.out.println("..........");
        for (int i = 0; i < diff.b.size(); i++) {
            System.out.println(b2sb1[i] + "," + diff.b.get(i));
        }

    }
}
