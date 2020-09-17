/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.avalanche;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;

import com.salesforce.apollo.fireflies.Member;
import com.salesforce.apollo.fireflies.View;

/**
 * @author hhildebrand
 *
 */
public class ViewSampler {

    private final Deque<Member> currentSet = new ConcurrentLinkedDeque<>();
    private final Random        entropy;
    private final View          view;

    public ViewSampler(View v, Random entropy) {
        view = v;
        this.entropy = entropy;
    }

    public List<Member> sample(int range) {
        int trueRange = Math.min(range, view.getView().size() - 1);
        if (trueRange == 0) {
            return Collections.emptyList();
        }
        List<Member> sample = new ArrayList<>();

        while (sample.size() < trueRange) {
            if (currentSet.isEmpty()) {
                List<Member> next = new ArrayList<>(view.getLive());
                Collections.shuffle(next, entropy);
                currentSet.addAll(next);
            }
            Member next = currentSet.poll();
            if (next == null) {
                return sample;
            }

            if (!next.equals(view.getNode())) {
                sample.add(next);
            }
        }

        return sample;
    }
}
