/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.comms;

import com.salesfoce.apollo.model.proto.Request;
import com.salesfoce.apollo.utils.proto.Sig;

/**
 * @author hal.hildebrand
 *
 */
public interface SigningService {
    Sig sign(Request request);
}
