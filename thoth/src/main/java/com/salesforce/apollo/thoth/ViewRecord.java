/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import java.util.List;

import org.h2.jdbcx.JdbcConnectionPool;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;

/**
 * Database management for view history
 *
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public class ViewRecord {
    private final JdbcConnectionPool connectionPool;
    private final Context<Member>    context;

    public ViewRecord(Context<Member> ctx, JdbcConnectionPool connectionPool) {
        context = ctx;
        this.connectionPool = connectionPool;
    }

    public void viewChange(Digest viewId, List<Digest> joins, List<Digest> leaves) {

    }
}
