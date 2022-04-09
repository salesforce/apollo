/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class MissingNode implements Node {
    final List<MaterializedNode> neededBy = new ArrayList<>();
    final Instant                requested;

    MissingNode(Instant requested) {
        this.requested = requested;
    }
}
