/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import com.salesfoce.apollo.proto.Submission;
import com.salesfoce.apollo.proto.SubmitResolution;

/**
 * @author hal.hildebrand
 *
 */
public interface Ordering {
    SubmitResolution submit(Submission request);
}
