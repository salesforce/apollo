/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.salesforce.apollo.avalanche.Avalanche;
import com.salesforce.apollo.membership.impl.CertWithKey;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class Subsystem {
    private final HashKey   id;
    private final Subsystem parent;
    private final CertWithKey credentials;
    private final Database management;
    private final Avalanche avalanche;

    public Subsystem(HashKey id, Subsystem parent, CertWithKey credentials, Database management, Avalanche avalanche) {
        this.id = id;
        this.parent = parent;
        this.credentials = credentials;
        this.management = management;
        this.avalanche = avalanche;
    }

    public Subsystem getParent() {
        return parent;
    }

    public HashKey getId() {
        return id;
    }
}
