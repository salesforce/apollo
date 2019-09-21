/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Provides a randomized sample of the membership using the rings of the underlying fireflies gossip graph as random
 * walks in the membership graph.
 * 
 * @author hal.hildebrand
 * @since 222
 */
public class RandomMemberGenerator {
    private volatile int currentRing = 0;
    private volatile Member lastMember;
    private final View view;

    public RandomMemberGenerator(View view) {
        this.view = view;
        lastMember = view.getNode();
    }

    /**
     * @return the next random member live member, or null if there ain't nobody alive other than this node
     */
    public Member next() {
        return next(m -> m.isLive());
    }

    /**
     * @param predicate
     *            - the selection predicate for members
     * @return the next random member member satisfying the predicate, or null if there ain't nobody other than this
     *         node that can satisfy said predicate
     */
    public Member next(Predicate<Member> predicate) {
        Set<Member> sample = sample(1, predicate);
        ArrayList<Member> sampled = new ArrayList<>(sample);
        return sampled.isEmpty() ? null : sampled.get(0);
    }

    /**
     * Answer a random sample of at least range size from the members of the group, not including the view's node
     * 
     * @param range
     *            - the desired range
     * @return a random sample set of the view's live members. May be limited by the number of available members.
     */
    public Set<Member> sample(int range) {
        return sample(range, m -> m.isLive());
    }

    /**
     * Answer a random sample of at least range size from the members of the group, not including the view's node
     * 
     * @param range
     *            - the desired range
     * @param predicate
     *            - the selection predicate for members
     * @return a random sample set of the view's members satisfying the predicate. May be limited by the number of
     *         available members.
     */
    public Set<Member> sample(int range, Predicate<Member> predicate) {
        int trueRange = Math.min(range, view.getView().size() - 1);
        if (trueRange == 0) { return Collections.emptySet(); }
        Set<Member> sample = new HashSet<Member>();

        Ring current = view.getRing(currentRing);
        while (sample.size() < trueRange) {
            Member next = current.successor(lastMember, m -> predicate.test(m));
            if (next != null) {
                lastMember = next;
                if (next.equals(view.getNode())) {
                    int nextRing = (current.getIndex() + 1) % view.getRings().size();
                    currentRing = nextRing;
                    current = view.getRing(nextRing);
                } else {
                    sample.add(next);
                }
            }
        }
        return sample;
    }
}
