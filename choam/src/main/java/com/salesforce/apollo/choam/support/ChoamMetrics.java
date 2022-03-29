/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.codahale.metrics.Timer;
import com.salesforce.apollo.ethereal.memberships.EtherealMetrics;
import com.salesforce.apollo.membership.messaging.rbc.RbcMetrics;
import com.salesforce.apollo.protocols.EdpointMetrics;

/**
 * @author hal.hildebrand
 *
 */
public interface ChoamMetrics extends EdpointMetrics {

    void publishedBatch(int batchSize, int byteSize, int validations);

    Timer transactionLatency();

    void transactionTimeout();

    void transactionComplete(Throwable t);

    void dropped(int transactions, int validations);

    void transactionSubmittedSuccess();

    void transactionSubmittedFail();

    void transactionSubmitRetry();

    RbcMetrics getCombineMetrics();

    EtherealMetrics getProducerMetrics();

    EtherealMetrics getReconfigureMetrics();

}
