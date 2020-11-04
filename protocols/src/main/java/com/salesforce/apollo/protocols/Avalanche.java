/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import com.salesfoce.apollo.proto.QueryResult;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Avalanche {

    QueryResult query(HashKey context, List<ByteBuffer> transactions, Collection<HashKey> wanted);

    List<byte[]> requestDAG(HashKey context, Collection<HashKey> want);

}
