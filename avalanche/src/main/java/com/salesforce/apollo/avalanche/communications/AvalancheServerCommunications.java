/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import java.nio.ByteBuffer;
import java.util.List;
 

import com.salesfoce.apollo.proto.QueryResult;
import com.salesforce.apollo.avalanche.Avalanche.Service; 
import com.salesforce.apollo.protocols.Avalanche;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheServerCommunications implements Avalanche {
    private final Service avalanche;

    public AvalancheServerCommunications(Service avalanche) {
        this.avalanche = avalanche;
    }

    @Override
    public QueryResult query(List<ByteBuffer> transactions, List<HashKey> wanted)   {
        return avalanche.onQuery(transactions, wanted);
    }

    @Override
    public List<ByteBuffer> requestDAG(List<HashKey> want)   {
        return avalanche.requestDAG(want);
    }
}
