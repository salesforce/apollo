/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model;

import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.Stereotomy;

/**
 * @author hal.hildebrand
 *
 */
public class Node {

    private final Digest     id;
    private final Shard      management;
    private final Stereotomy stereotomy;
    private final Router     router;

    public Node(Digest id, Shard management, Stereotomy stereotomy, Router router) {
        this.id = id;
        this.management = management;
        this.stereotomy = stereotomy;
        this.router = router;
    }

    public Digest getId() {
        return id;
    }
}
