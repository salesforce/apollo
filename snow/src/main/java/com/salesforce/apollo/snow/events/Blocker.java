/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.events;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class Blocker {

    private final Map<ID, Collection<Blockable>> blockers = new HashMap<>();

    public void fulfill(ID id) {
        Collection<Blockable> blocking = blockers.remove(id);
        if (blocking != null) {
            blocking.forEach(e -> e.fulfill(id));
        }
    }

    public void abandon(ID id) {
        Collection<Blockable> blocking = blockers.remove(id);
        if (blocking != null) {
            blocking.forEach(e -> e.fulfill(id));
        }
    }

    public void register(Blockable pending) {
        pending.dependencies().forEach(id -> blockers.computeIfAbsent(id, i -> new ArrayList<>()).add(pending));
        pending.update();
    }

}
