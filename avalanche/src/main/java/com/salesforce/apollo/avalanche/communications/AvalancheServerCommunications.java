/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.avalanche.communications;

import java.util.List;

import org.apache.avro.AvroRemoteException;

import com.salesforce.apollo.avalanche.Avalanche.Service;
import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.avro.QueryResult;
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
    public QueryResult query(List<HASH> transactions, List<HASH> want) throws AvroRemoteException {
        return avalanche.onQuery(transactions, want);
    }

    @Override
    public List<Entry> requestDAG(List<HASH> want) throws AvroRemoteException {
        return avalanche.requestDAG(want);
    }
}
