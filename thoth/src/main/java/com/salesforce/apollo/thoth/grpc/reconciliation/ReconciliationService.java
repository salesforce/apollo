/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.reconciliation;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.thoth.proto.Intervals;
import com.salesfoce.apollo.thoth.proto.Update;
import com.salesfoce.apollo.thoth.proto.Updating;
import com.salesforce.apollo.archipelago.Link;

/**
 * @author hal.hildebrand
 *
 */
public interface ReconciliationService extends Link {

    ListenableFuture<Update> reconcile(Intervals intervals);

    ListenableFuture<Empty> update(Updating update);

}
