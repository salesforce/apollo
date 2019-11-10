/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.AvroRemoteException;

import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.avro.QueryResult;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Avalanche {

    QueryResult query(List<ByteBuffer> transactions, List<HASH> wanted) throws AvroRemoteException;

    List<ByteBuffer> requestDAG(List<HASH> want) throws AvroRemoteException;

}
