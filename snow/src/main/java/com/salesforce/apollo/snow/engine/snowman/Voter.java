/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.engine.snowman;

import java.util.Set;

import com.salesforce.apollo.snow.ids.ID;
import com.salesforce.apollo.snow.ids.ShortID;

/**
 * @author hal.hildebrand
 *
 */
public class Voter {
    private final Transitive t;
    private final ShortID    vdr;
    private final int        requestID;
    private final ID         response;
    private final Set<ID>    deps;

    public Voter(Transitive t, ShortID vdr, int requestID, ID response, Set<ID> deps) {
        this.t = t;
        this.vdr = vdr;
        this.requestID = requestID;
        this.response = response;
        this.deps = deps;
    }

    public Set<ID> dependencies() {
        return deps;
    }
    
    public void fulfill(ID id) {
        deps.remove(id);
        update();
    }
    
    public void update() {
        if (!deps.isEmpty()) {
            return;
        } 
    }
}
