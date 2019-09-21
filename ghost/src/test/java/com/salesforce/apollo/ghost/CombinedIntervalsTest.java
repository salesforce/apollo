/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.ghost;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class CombinedIntervalsTest {

    @Test
    public void smoke() {
        List<KeyInterval> intervals = new ArrayList<>();
        intervals.add(new KeyInterval(new HashKey(new byte[] { 0, (byte)200 }),
                                      new HashKey(new byte[] { 0, (byte)241 })));
        intervals.add(new KeyInterval(new HashKey(new byte[] { 0, 50 }), new HashKey(new byte[] { 0, 75 })));
        intervals.add(new KeyInterval(new HashKey(new byte[] { 0, 50 }), new HashKey(new byte[] { 0, 90 })));
        intervals.add(new KeyInterval(new HashKey(new byte[] { 0, 25 }), new HashKey(new byte[] { 0, 49 })));
        intervals.add(new KeyInterval(new HashKey(new byte[] { 0, 25 }), new HashKey(new byte[] { 0, 49 })));
        intervals.add(new KeyInterval(new HashKey(new byte[] { 0, (byte)128 }),
                                      new HashKey(new byte[] { 0, (byte)175 })));
        CombinedIntervals combined = new CombinedIntervals(intervals);
        List<KeyInterval> compressed = combined.getIntervals();
        System.out.println(compressed);
        assertEquals(4, compressed.size());
    }
}
