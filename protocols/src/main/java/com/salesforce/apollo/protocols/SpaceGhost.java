/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.protocols;

import java.util.List;

import org.apache.avro.AvroRemoteException;

import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.GhostUpdate;
import com.salesforce.apollo.avro.Interval;
import com.salesforce.apollo.avro.HASH;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface SpaceGhost {
    Entry get(HASH key) throws org.apache.avro.AvroRemoteException;

    void put(Entry entry);

    GhostUpdate ghostGossip(List<Interval> intervals, List<HASH> digests) throws AvroRemoteException;

    void gUpdate(java.util.List<Entry> updates);
}
