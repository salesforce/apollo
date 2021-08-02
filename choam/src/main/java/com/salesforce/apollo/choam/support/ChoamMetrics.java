/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.support;

import com.salesfoce.apollo.choam.proto.Coordinate;
import com.salesforce.apollo.comm.RouterMetrics;
import com.salesforce.apollo.ethereal.Ethereal.PreBlock;
import com.salesforce.apollo.ethereal.PreUnit;

/**
 * @author hal.hildebrand
 *
 */
public interface ChoamMetrics extends RouterMetrics {

    void publishedBatch(int batchSize, int byteSize);

    void invalidUnit();

    void preBlockProduced(PreBlock preblock);

    void coordination(Coordinate coordination);

    void broadcast(PreUnit preUnit);

    void invalidSourcePid();

    void coordDeserEx();

    void incTotalMessages();

}
