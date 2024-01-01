/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.demesnes.comm;

import com.salesforce.apollo.demesne.proto.SubContext;
import com.salesforce.apollo.cryptography.proto.Digeste;

/**
 * @author hal.hildebrand
 */
public interface OuterContextService {
    void deregister(Digeste context);

    void register(SubContext context);
}
