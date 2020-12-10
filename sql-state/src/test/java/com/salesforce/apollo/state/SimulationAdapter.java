/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.util.function.Function;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.consortium.EnqueuedTransaction;

/**
 * @author hal.hildebrand
 *
 */
public class SimulationAdapter implements Function<EnqueuedTransaction, ByteString> {

    @Override
    public ByteString apply(EnqueuedTransaction t) {
        // TODO Auto-generated method stub
        return null;
    }

}
