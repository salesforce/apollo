/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc.reconciliation;

import com.google.protobuf.Empty;
import com.salesforce.apollo.thoth.proto.Intervals;
import com.salesforce.apollo.thoth.proto.Update;
import com.salesforce.apollo.thoth.proto.Updating;
import com.salesforce.apollo.archipelago.Link;

/**
 * @author hal.hildebrand
 */
public interface ReconciliationService extends Link {

    Update reconcile(Intervals intervals);

    Empty update(Updating update);

}
