/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.avalanche.communications.gprc;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.apache.avro.AvroRemoteException;

import com.salesfoce.apollo.proto.AvalancheGrpc.AvalancheImplBase;
import com.salesfoce.apollo.proto.DagNodes;
import com.salesfoce.apollo.proto.Query;
import com.salesforce.apollo.avro.QueryResult;
import com.salesforce.apollo.protocols.Avalanche;
import com.salesforce.apollo.protocols.HashKey;

import io.grpc.stub.StreamObserver;

/**
 * @author hal.hildebrand
 * @since 220
 */
public class AvalancheClientCommunications extends AvalancheImplBase implements Avalanche {

    public void close() {
    }

    @Override
    public void query(Query request, StreamObserver<com.salesfoce.apollo.proto.QueryResult> responseObserver) {
        // TODO Auto-generated method stub
        super.query(request, responseObserver);
    }

    @Override
    public void requestDag(DagNodes request, StreamObserver<DagNodes> responseObserver) {
        // TODO Auto-generated method stub
        super.requestDag(request, responseObserver);
    }

    @Override
    public QueryResult query(List<ByteBuffer> transactions, Collection<HashKey> wanted) throws AvroRemoteException {
        return null;
    }

    @Override
    public List<ByteBuffer> requestDAG(Collection<HashKey> want) throws AvroRemoteException {
        return null;
    }
}
