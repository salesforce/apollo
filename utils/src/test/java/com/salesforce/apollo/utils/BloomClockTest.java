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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.bloomFilters.BloomClock;
import com.salesforce.apollo.utils.bloomFilters.BloomClock.ClockValueComparator;
import com.salesforce.apollo.utils.bloomFilters.ClockValue;

/**
 * @author hal.hildebrand
 */
public class BloomClockTest {
    private static final long SEED = -772069189919430497L;

    record Event(BloomClock clockValue, Digest hash, int timestamp) {
    }

    record Node(Map<Integer, Event> history, AtomicReference<BloomClock> state, Comparator<ClockValue> comparator) {

        BloomClock currentState() {
            return state.get();
        }

        void process(Event event) {
            BloomClock current = state.get();
            int comparison = comparator.compare(event.clockValue, current);
            if (comparison < 0) {
                return; // event in the past, discarding
            }
            if (comparison > 0) { // event from our future
                current.merge(event.clockValue);
                history.put(event.timestamp, event);
            }
            // We can't compare the event with this node's clock, so we have to fall back to
            // timestamps
            int localMax = history.keySet().stream().mapToInt(e -> (Integer) e).max().getAsInt();
            if (event.timestamp > localMax) { // Event's timestamp wins
                current.merge(event.clockValue);
                history.put(event.timestamp, event);
            } else if (event.timestamp == localMax) { // no winner, lexically order e.g.
                if (history.get(localMax).hash.compareTo(event.hash) <= 0) { // event wins
                    current.merge(event.clockValue);
                    history.put(event.timestamp, event);
                }
            }
            // Event was "definitely" in the past and discarded
        }
    }

    Digest digest(Random entropy) {
        byte[] buf = new byte[DigestAlgorithm.DEFAULT.digestLength()];
        entropy.nextBytes(buf);
        Digest digest = new Digest(DigestAlgorithm.DEFAULT, buf);
        return digest;
    }

    Event newEvent(Node node, Digest digest, int timestamp) {
        BloomClock clock = node.currentState();
        clock.add(digest);
        return new Event(clock, digest, timestamp);
    }

    @Test
    public void paperScenario() throws Exception {
        Comparator<ClockValue> comparator = new ClockValueComparator(0.1);
        AtomicInteger clock = new AtomicInteger();
        int K = 3;
        int M = 200;
        BloomClock startState = new BloomClock(SEED, K, M);

        Node a = new Node(new HashMap<>(), new AtomicReference<>(startState.clone()), comparator);
        Node b = new Node(new HashMap<>(), new AtomicReference<>(startState.clone()), comparator);
        Node c = new Node(new HashMap<>(), new AtomicReference<>(startState.clone()), comparator);
        Node d = new Node(new HashMap<>(), new AtomicReference<>(startState.clone()), comparator);
        Node e = new Node(new HashMap<>(), new AtomicReference<>(startState.clone()), comparator);

        final Event t1, t2, t3, t4, t5;
        Random entropy = new Random(SEED);

        // Node A originates event at t1
        t1 = newEvent(a, digest(entropy), clock.incrementAndGet());

        // Delivered only to B,D,E
        b.process(t1);
        d.process(t1);
        e.process(t1);

        assertEquals(t1.clockValue, a.currentState());
        assertEquals(t1.clockValue, b.currentState());
        assertEquals(t1.clockValue, d.currentState());
        assertEquals(t1.clockValue, e.currentState());

        // Node B originates event at t2
        t2 = newEvent(b, digest(entropy), clock.incrementAndGet());

        // Delivered only to A, E
        a.process(t2);
        e.process(t2);

        assertEquals(t2.clockValue, b.currentState());
        assertEquals(t2.clockValue, a.currentState());
        assertEquals(t2.clockValue, e.currentState());

        // Node D originates event at t3
        t3 = newEvent(d, digest(entropy), clock.incrementAndGet());

        // Delivered only to C, E
        c.process(t3);
        e.process(t3);

        assertEquals(t3.clockValue, d.currentState());
        assertEquals(t3.clockValue, c.currentState());
        assertEquals(-1, comparator.compare(t3.clockValue, e.currentState()));

        // Node E cannot decide, but timestamps on the events wins and thus event t3 is
        // "in the past" for node E
        assertEquals(-1, comparator.compare(t3.clockValue, e.currentState()));

        // Node E originates event at t4
        t4 = newEvent(e, digest(entropy), clock.incrementAndGet());

        // Delivered only to D
        d.process(t4);

        assertEquals(-1, comparator.compare(t4.clockValue, e.currentState()));
        assertEquals(t4.clockValue, d.currentState());

        // Node C originates event at t4
        t5 = newEvent(c, digest(entropy), clock.incrementAndGet());

        // Delivered only to D
        d.process(t5);

        assertEquals(t5.clockValue, c.currentState());
        assertEquals(-1, comparator.compare(t5.clockValue, d.currentState()));
    }

    @Test
    public void smokin() {
        Comparator<ClockValue> comparator = new ClockValueComparator(0.1);
        BloomClock a = new BloomClock(new int[] { 1, 0, 0, 1 });
        BloomClock b = new BloomClock(new int[] { 1, 1, 0, 1 });
        BloomClock c = new BloomClock(new int[] { 1, 1, 0, 0 });
        BloomClock d = new BloomClock(new int[] { 1, 1, 1, 1 });

        // A proceeds B
        assertEquals(-1, comparator.compare(a, b));
        // B occurs after A
        assertEquals(1, comparator.compare(b, a));
        // A and C are simultaneous - not comparable
        assertEquals(0, comparator.compare(a, c));
        // D occurs after A
        assertEquals(1, comparator.compare(d, a));
    }
}
