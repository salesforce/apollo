/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import java.util.List;

import org.apache.avro.AvroRemoteException;

import com.salesforce.apollo.avro.DagEntry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.avro.QueryResult;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Avalanche {

    QueryResult query(List<DagEntry> transactions) throws AvroRemoteException;

    List<DagEntry> requestDAG(List<HASH> want) throws AvroRemoteException;

}
