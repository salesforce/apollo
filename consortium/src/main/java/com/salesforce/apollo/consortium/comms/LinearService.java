/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.comms;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.consortium.proto.Join;
import com.salesfoce.apollo.consortium.proto.JoinResult;
import com.salesfoce.apollo.consortium.proto.StopData;
import com.salesfoce.apollo.consortium.proto.SubmitTransaction;
import com.salesfoce.apollo.consortium.proto.TransactionResult;

/**
 * @author hal.hildebrand
 *
 */
public interface LinearService {

    ListenableFuture<TransactionResult> clientSubmit(SubmitTransaction request);

    ListenableFuture<JoinResult> join(Join join);

    void stopData(StopData stopData);
}
