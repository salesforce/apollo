/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static com.salesforce.apollo.stereotomy.schema.Tables.IDENTIFIER;
import static com.salesforce.apollo.thoth.schema.Tables.MEMBER;
import static com.salesforce.apollo.thoth.schema.Tables.MEMBERSHIP;
import static com.salesforce.apollo.thoth.schema.Tables.VIEW;
import static org.jooq.impl.DSL.param;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.h2.jdbcx.JdbcConnectionPool;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * Database management for view history
 *
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("unused")
public class ViewAccounting {
    private final Digest                  ctxId;
    private final AtomicReference<Digest> currentView = new AtomicReference<>();
    private final DSLContext              dsl;

    public ViewAccounting(Digest ctxId, JdbcConnectionPool connectionPool) {
        this.ctxId = ctxId;
        dsl = DSL.using(connectionPool, SQLDialect.H2);
    }

    public List<Identifier> successors(Digest digest) {
        List<Identifier> successors = new ArrayList<>();
        Class<byte[]> byteClass = null;
        final var k = param("k", Short.class);
        final var dBytes = digest.getBytes();
        var fetch = dsl.select(IDENTIFIER.PREFIX)
                       .from(MEMBER)
                       .join(VIEW)
                       .on(VIEW.DIGEST.eq(dBytes))
                       .join(IDENTIFIER)
                       .on(IDENTIFIER.ID.eq(MEMBER.IDENTIFIER))
                       .join(MEMBERSHIP)
                       .on(MEMBERSHIP.VIEW.eq(VIEW.ID).and(MEMBERSHIP.RING.eq(k)).and(MEMBERSHIP.MEMBER.eq(MEMBER.ID)))
                       .where(MEMBER.RING.eq(k).and(MEMBER.DIGEST.gt(param("hash", byteClass))))
                       .limit(1)
                       .keepStatement(true);
        try {
            final var K = dsl.select(VIEW.K).from(VIEW).where(VIEW.DIGEST.eq(dBytes)).fetchOne().value1();
            for (int i = 0; i < K; i++) {
                fetch.bind("k", i);
                fetch.bind("hash", Context.hashFor(ctxId, i, digest).getBytes());
                try {
                    successors.add(Identifier.from(Ident.parseFrom(fetch.fetchOne().value1())));
                } catch (InvalidProtocolBufferException | DataAccessException e) {
                    throw new IllegalStateException(e);
                }
            }
            return successors;
        } finally {
            fetch.close();
        }
    }

    public void viewChange(Context<Member> context, Digest viewId, List<Digest> joins, List<Digest> leaves) {

    }
}
