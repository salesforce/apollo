/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
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
