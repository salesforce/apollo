/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.utils.bloomFilters.BloomClock;
import com.salesforce.apollo.utils.bloomFilters.BloomClock.BcComparator;
import com.salesforce.apollo.utils.bloomFilters.BloomClock.ClockOverflowException;

/**
 * @author hal.hildebrand
 */
public class BloomClockTest {

    private final Comparator<BloomClock> comparator = new BcComparator(0.1);

    @Test
    public void smokin() {
        BloomClock a = new BloomClock(0x1638, new int[] { 1, 0, 0, 1 }, 3);
        BloomClock b = new BloomClock(0x1638, new int[] { 1, 1, 0, 1 }, 3);
        BloomClock c = new BloomClock(0x1638, new int[] { 1, 1, 0, 0 }, 3);
        BloomClock d = new BloomClock(0x1638, new int[] { 1, 1, 1, 1 }, 3);

        // A proceeds B
        assertEquals(-1, comparator.compare(a, b));
        // B occurs after A
        assertEquals(1, comparator.compare(b, a));
        // A and C are simultaneous - not comparable
        assertEquals(0, comparator.compare(a, c));
        // D occurs after A
        assertEquals(1, comparator.compare(d, a));
    }

    @Test
    public void paperScenario() throws Exception {

        // Example of the causal processing of events partially ordered by Bloom Clocks
        record Node(Map<Integer, BloomClock> history, AtomicReference<BloomClock> state) {
            Node initialState() {
                history.put(0, state.get().clone());
                return this;
            }

            BloomClock currentState() {
                return state.get();
            }

            void process(BloomClock event, int t) throws ClockOverflowException {
                state.get().mergeWith(event);
                history.put(t, state.get().clone());
            }
        }

        BloomClock startState = event(new int[] { 0, 0, 0, 0, 0, 0, 0, 0 });
        Node a = new Node(new HashMap<>(), new AtomicReference<>(startState.clone())).initialState();
        Node b = new Node(new HashMap<>(), new AtomicReference<>(startState.clone())).initialState();
        Node c = new Node(new HashMap<>(), new AtomicReference<>(startState.clone())).initialState();
        Node d = new Node(new HashMap<>(), new AtomicReference<>(startState.clone())).initialState();
        Node e = new Node(new HashMap<>(), new AtomicReference<>(startState.clone())).initialState();

        BloomClock e1, e2, e3, e4, e5;

        e1 = event(new int[] { 0, 1, 0, 0, 1, 0, 0, 0 });

        a.process(e1, 1); // orginates e1
        b.process(e1, 1);
        d.process(e1, 1);
        e.process(e1, 1);

        assertEquals(e1, a.currentState());
        assertEquals(e1, b.currentState());
        assertEquals(e1, d.currentState());
        assertEquals(e1, e.currentState());

        e2 = event(new int[] { 0, 2, 0, 0, 1, 0, 1, 0 });

        b.process(e2, 2); // orginates e2
        a.process(e2, 2);
        e.process(e2, 2);

        assertEquals(e2, b.currentState());
        assertEquals(e2, a.currentState());
        assertEquals(e2, e.currentState());

        e3 = event(new int[] { 1, 2, 0, 0, 1, 0, 0, 0 });

        d.process(e3, 3); // orginates e3
        c.process(e3, 3);
        e.process(e3, 3);

        assertEquals(e3, d.currentState());
        assertEquals(e3, c.currentState());
        assertEquals(event(new int[] { 1, 2, 0, 0, 1, 0, 1, 0 }), e.currentState());

        e4 = event(new int[] { 2, 2, 0, 0, 1, 1, 0, 0 });

        c.process(e4, 4); // orginates e4
        d.process(e4, 4);

        assertEquals(e4, c.currentState());
        assertEquals(e4, d.currentState());
        
        e5 = event(new int[] { 2, 2, 1, 0, 1, 1, 1, 0 });

        c.process(e5, 5); // orginates e5
        b.process(e5, 5);

        assertEquals(e5, c.currentState());
        assertEquals(e5, b.currentState());
    }

    BloomClock event(int[] vector) {
        return new BloomClock(0x1638, vector, 3);
    }
}
