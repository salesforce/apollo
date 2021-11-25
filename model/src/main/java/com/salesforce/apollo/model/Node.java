/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import java.util.HashMap;
import java.util.Map;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class Node {

    private final Map<Digest, Subsystem> hosted = new HashMap<>();
    private final Digest                 id;
    private final Database               management;

    public Node(Digest id, Database management) {
        this.id = id;
        this.management = management;
    }

    public Digest getId() {
        return id;
    }
}
