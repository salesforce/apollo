/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.math3.util.Pair;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.salesfoce.apollo.proto.QueryResult;
import com.salesfoce.apollo.proto.SuppliedDagNodes;
import com.salesforce.apollo.comm.Link;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface AvalancheService extends Link {

    static AvalancheService getLocalLoopback(SigningMember node) {
        return new AvalancheService() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public Member getMember() {
                return node;
            }

            @Override
            public ListenableFuture<QueryResult> query(Digest context, List<Pair<Digest, ByteString>> transactions,
                                                       Collection<Digest> wanted) {
                return null;
            }

            @Override
            public ListenableFuture<SuppliedDagNodes> requestDAG(Digest context, Collection<Digest> want) {
                return null;
            }
        };
    }

    ListenableFuture<QueryResult> query(Digest context, List<Pair<Digest, ByteString>> transactions,
                                        Collection<Digest> wanted);

    ListenableFuture<SuppliedDagNodes> requestDAG(Digest context, Collection<Digest> want);

}
