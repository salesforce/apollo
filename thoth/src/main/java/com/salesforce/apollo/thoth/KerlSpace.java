/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import java.util.Collections;
import java.util.List;

import org.h2.jdbcx.JdbcConnectionPool;

import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.thoth.proto.Intervals;
import com.salesfoce.apollo.thoth.proto.Update;
import com.salesforce.apollo.crypto.Digest;

/**
 * Represents the replicated KERL logic
 * 
 * @author hal.hildebrand
 *
 */
public class KerlSpace {
    @SuppressWarnings("unused")
    private final JdbcConnectionPool connectionPool;

    public KerlSpace(JdbcConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    /**
     * Answer all the hashes for the identifiers contained within the keyIntervals
     * 
     * @param keyIntervals
     * @return the List of Digests bounded by the key intervals
     */
    public List<Digest> populate(CombinedIntervals keyIntervals) {
        // TODO Auto-generated method stub
        return Collections.emptyList();
    }

    /**
     * Reconcile the intervals for our partner
     * 
     * @param intervals - the relevant intervals of identifiers and the event
     *                  digests of these identifiers the partner already have
     * @return the Update of missing key events, as well as the intervals the
     *         receiver cares about, and the biff of the identifiers' event hashes
     */
    public Update reconcile(Intervals intervals) {
        // TODO Auto-generated method stub
        return Update.getDefaultInstance();
    }

    /**
     * Update the key events in this space
     * 
     * @param events
     */
    public void update(List<KeyEvent_> events) {
        // TODO Auto-generated method stub

    }
}
