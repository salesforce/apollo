/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.thoth.proto.Entries;
import com.salesfoce.apollo.thoth.proto.Intervals;
import com.salesforce.apollo.comm.Link;

/**
 * @author hal.hildebrand
 *
 */
public interface ReconciliationService extends Link {

    ListenableFuture<Entries> intervals(Intervals build);

}
