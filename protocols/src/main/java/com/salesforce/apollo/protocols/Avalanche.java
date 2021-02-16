/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.util.Collection;
import java.util.List;

import org.apache.commons.math3.util.Pair;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.SuppliedDagNodes;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Avalanche {

    ListenableFuture<QueryResult> query(HashKey context, List<Pair<HashKey, ByteString>> transactions, Collection<HashKey> wanted);

    ListenableFuture<SuppliedDagNodes> requestDAG(HashKey context, Collection<HashKey> want);

}
