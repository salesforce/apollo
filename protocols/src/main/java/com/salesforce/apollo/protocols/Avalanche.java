/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.util.Collection;
import java.util.List;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.ID;
import com.salesfoce.apollo.proto.QueryResult;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Avalanche {

    QueryResult query(HashKey context, List<ID> transactions, Collection<HashKey> wanted);

    QueryResult requery(HashKey context, List<ByteString> transactions);

    List<ByteString> requestDAG(HashKey context, Collection<HashKey> want);

}
