/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public class Subsystem {
    private final Digest    id;
    private final Subsystem parent;
    private final Database  management;

    public Subsystem(Digest id, Subsystem parent, Database management) {
        this.id = id;
        this.parent = parent;
        this.management = management;
    }

    public Subsystem getParent() {
        return parent;
    }

    public Digest getId() {
        return id;
    }
}
