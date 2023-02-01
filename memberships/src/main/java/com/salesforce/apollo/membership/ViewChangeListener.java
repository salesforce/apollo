/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.util.List;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface ViewChangeListener {
    void viewChange(Digest viewId, List<Digest> joins, List<Digest> leaves);
}
