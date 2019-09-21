/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.ghost;

import java.util.List;

import com.salesforce.apollo.avro.Entry;
import com.salesforce.apollo.avro.GhostUpdate;
import com.salesforce.apollo.avro.HASH;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 * @since 220
 */
public interface Store {

    public List<HASH> keySet();

    Entry get(HASH key);

    void put(HASH key, Entry value);

    GhostUpdate updatesFor(CombinedIntervals theirIntervals, List<HashKey> digests,
            CombinedIntervals combinedIntervals);

}
