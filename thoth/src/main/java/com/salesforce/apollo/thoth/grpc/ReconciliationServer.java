/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth.grpc;

import com.salesfoce.apollo.thoth.proto.ReconciliationGrpc.ReconciliationImplBase;
import com.salesforce.apollo.comm.RoutableService;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;

/**
 * @author hal.hildebrand
 *
 */
public class ReconciliationServer extends ReconciliationImplBase {

    private final StereotomyMetrics               metrics;
    private final RoutableService<Reconciliation> routing;

    public ReconciliationServer(StereotomyMetrics metrics, RoutableService<Reconciliation> router) {
        this.metrics = metrics;
        this.routing = router;
    }

}
