/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import org.apache.avro.AvroRemoteException;

import com.salesforce.apollo.avro.Digests;
import com.salesforce.apollo.avro.Gossip;
import com.salesforce.apollo.avro.Signed;
import com.salesforce.apollo.avro.Update;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Fireflies { 
    
    Gossip gossip(Signed note, int ring, Digests gossip) throws AvroRemoteException;

    int ping(int ping) throws AvroRemoteException;

    void update(int ring, Update update) throws AvroRemoteException;

}
