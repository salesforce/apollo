/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import static com.salesforce.apollo.delphinius.schema.tables.Edge.EDGE;
import static com.salesforce.apollo.delphinius.schema.tables.Namespace.NAMESPACE;
import static com.salesforce.apollo.delphinius.schema.tables.Object.OBJECT;
import static com.salesforce.apollo.delphinius.schema.tables.Relation.RELATION;
import static com.salesforce.apollo.delphinius.schema.tables.Subject.SUBJECT;

import java.sql.Connection;
import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * An Access Control Oracle
 * 
 * @author hal.hildebrand
 *
 */
public class Oracle {

    private final DSLContext dslCtx;

    public Oracle(DSLContext dslCtx) {
        this.dslCtx = dslCtx;
    }

    public Oracle(Connection connection) {
        this(DSL.using(connection));
    }

    public void addTuple(String namespace, String p, String r, String c) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            var resolved = context.select(OBJECT.ID, RELATION.ID, SUBJECT.ID).from(OBJECT, RELATION, SUBJECT)
                                  .join(OBJECT).on(NAMESPACE.ID.eq(OBJECT.NAMESPACE)).where(OBJECT.NAME.eq(p))
                                  .and(RELATION.NAME.eq(r)).and(SUBJECT.NAME.eq(c)).fetchOne();

            var parent = resolved.value1();
            var relation = resolved.value2();
            var child = resolved.value3();

            if (context.fetchExists(context.select(EDGE.ID).from(EDGE).where(EDGE.START.eq(parent))
                                           .and(EDGE.END.eq(child)).and(EDGE.HOPS.eq(0)))) {
                return;
            }
            if (parent == child || context.fetchExists(context.select(EDGE.ID).from(EDGE).where(EDGE.START.eq(parent))
                                                              .and(EDGE.END.eq(child)))) {
                throw new SQLException(String.format("Cycle inserting: %s rel: %s to: %s", parent, relation, child));
            }
            var id = context.insertInto(EDGE).columns(EDGE.START, EDGE.RELATION, EDGE.END, EDGE.HOPS)
                            .values(parent, relation, child, 0).returning().fetchOne().getId();

            context.update(EDGE).set(EDGE.ENTRY, id).set(EDGE.EXIT, id).set(EDGE.DIRECT, id).execute();

            // parent's incoming edges to child
            context.insertInto(EDGE).columns(EDGE.ENTRY, EDGE.DIRECT, EDGE.EXIT, EDGE.START, EDGE.END, EDGE.HOPS)
                   .select(context.select(DSL.val(id), DSL.val(id), EDGE.ID, EDGE.START, EDGE.END, EDGE.HOPS.plus(1))
                                  .from(EDGE).where(EDGE.END.eq(child)));

            // parent to child's outgoing edges
            context.insertInto(EDGE).columns(EDGE.ENTRY, EDGE.DIRECT, EDGE.EXIT, EDGE.START, EDGE.END, EDGE.HOPS)
                   .select(context.select(EDGE.ID, DSL.val(id), DSL.val(id), EDGE.START, DSL.val(child),
                                          EDGE.HOPS.plus(1))
                                  .from(EDGE).where(EDGE.END.eq(child)));

            // parent's incoming edges to end vertex of child's outgoing edges
            var A = EDGE.as("parent");
            var B = EDGE.as("child");
            context.insertInto(EDGE).columns(EDGE.ENTRY, EDGE.DIRECT, EDGE.EXIT, EDGE.START, EDGE.END, EDGE.HOPS)
                   .select(context.select(A.field(EDGE.ID), DSL.val(id), B.field(EDGE.ID), A.field(EDGE.START),
                                          B.field(EDGE.END), A.field(EDGE.HOPS).plus(B.field(EDGE.HOPS)))
                                  .from(A).crossJoin(B).where(A.field(EDGE.END).eq(parent))
                                  .and(B.field(EDGE.START).eq(child)));
        });
    }

    public void delete(String namespace, String p, String r, String c) {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            var resolved = context.select(OBJECT.ID, RELATION.ID, SUBJECT.ID).from(OBJECT, RELATION, SUBJECT)
                                  .join(OBJECT).on(NAMESPACE.ID.eq(OBJECT.NAMESPACE)).where(OBJECT.NAME.eq(p))
                                  .and(RELATION.NAME.eq(r)).and(SUBJECT.NAME.eq(c)).fetchOne();
            if (resolved == null) {
                throw new SQLException(String.format("Edge object: %s:%s rel: %s subject: %s does not exist", namespace,
                                                     p, r, c));
            }

            var parent = resolved.value1();
            var relation = resolved.value2();
            var child = resolved.value3();

            var idR = context.select(EDGE.ID).from(EDGE).where(EDGE.START.eq(parent))
                             .and(EDGE.RELATION.eq(relation).and(EDGE.END.eq(child)).and(EDGE.HOPS.eq(0))).fetchOne();
            if (idR == null) {
                throw new SQLException(String.format("Edge object: %s:%s rel: %s subject: %s does not exist", namespace,
                                                     p, r, c));
            }
            var id = idR.value1();

            // Temporary table to track the purged
            Table<org.jooq.Record> purgeList = DSL.table(DSL.name("purge_list"));
            Field<Long> pID = DSL.field(DSL.name("purge_list", "id"), Long.class);
            context.createTemporaryTable(purgeList);

            // Rows that were originally inserted with the first add() call for this direct
            // edge
            context.insertInto(purgeList).select(context.select(EDGE.ID).from(EDGE).where(EDGE.DIRECT.eq(id)))
                   .execute();

            // Scan and find all dependent rows that are inserted afterwards
            while (true) {
                if (context.insertInto(purgeList)
                           .select(context.select(EDGE.ID).from(EDGE)
                                          .where(EDGE.HOPS.gt(0)
                                                          .and(EDGE.ENTRY.in(context.select(pID).from(purgeList))
                                                                         .or(EDGE.EXIT.in(context.select(pID)
                                                                                                 .from(purgeList)))))
                                          .and(EDGE.ID.notIn(context.select(pID).from(purgeList))))
                           .execute() == 0) {
                    break;
                }
            }

            context.delete(EDGE).where(EDGE.ID.in(context.select(pID).from(purgeList))).execute();
            context.dropTable(purgeList);
        });
    }
}
