/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils.bloomFilters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;
import com.salesforce.apollo.utils.Utils;
import com.salesforce.apollo.utils.bloomFilters.IBF.Decode;

/**
 * @author hal.hildebrand
 *
 */
public class LongIBFTest {

    private Random    r;
    private IBF<Long> ibf;

    @BeforeEach
    public void before() {
        r = new Random(0x1638);
        ibf = new IBF.LongIBF(r.nextInt(), 2000, 3);
    }

    @Test
    public void add() {
        List<Long> inserted = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            long val = r.nextInt(9999);
            inserted.add(val);
            ibf.add(val);
        }
        for (long tst : inserted) {
            assert (ibf.contains(tst));
        }
    }

    @Test
    public void pure() throws Exception {
        long val = r.nextInt(9999);
        ibf.add(val);
        int count = 0;
        for (int cell = 0; cell < ibf.getM(); cell++) {
            if (ibf.isPure(cell))
                count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void decode() throws Exception {

        int seed1 = 31;
        IBF<Long> b1 = new IBF.LongIBF(seed1, 200, 3);
        IBF<Long> b2 = new IBF.LongIBF(seed1, 200, 3);

        for (int i = 0; i < 50000; i++) {
            long val = r.nextInt(99999999);
            b1.add(val);
            b2.add(val);
        }

        long b1sb2[] = new long[10];
        long b2sb1[] = new long[20];

        for (int i = 0; i < b1sb2.length; i++) {
            long val = r.nextInt(99999999);
            b1.add(val);
            b1sb2[i] = val;
        }
        for (int i = 0; i < b2sb1.length; i++) {
            long val = r.nextInt(99999999);
            b2.add(val);
            b2sb1[i] = val;
        }

        Arrays.sort(b1sb2);
        Arrays.sort(b2sb1);

        Decode<Long> diff = b1.subtract(b2).decode();
        assertNotNull(diff);
        assertEquals(b1sb2.length, diff.added().size(), "incorrect added");
        assertEquals(b2sb1.length, diff.missing().size(), "incorrect missing");

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

    @Test
    public void list() {
        r = new SecureRandom();
        
        List<IBF<Long>> ibfs = new ArrayList<>();
        int expected = 1500;
        Set<Long> state = new HashSet<>();
        while (state.size() < expected) {
            state.add(r.nextLong());
        }

        for (int i = 0; i < 5; i++) {
            ibfs.add(new IBF.LongIBF(Utils.bitStreamEntropy().nextLong(), expected , 3));
        }
        
        for (Long l: state) {
            ibfs.forEach(i -> i.add(l));
        }

        Set<Long> recovered = new HashSet<>();
        ibfs.forEach(ibf -> recovered.addAll(ibf.list()));
        ibfs.forEach(ibf -> assertEquals(0, ibf.list().size()));
        assertEquals(state.size(), recovered.size()); 
        assertEquals(0, Sets.difference(state, recovered).size());
    }
}
