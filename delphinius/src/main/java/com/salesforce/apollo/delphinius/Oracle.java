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

    public Oracle(Connection connection) {
        this(DSL.using(connection));
    }

    public Oracle(DSLContext dslCtx) {
        this.dslCtx = dslCtx;
    }

    public void addTuple(String n, String p, String c) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            Table<org.jooq.Record> candidates = DSL.table(DSL.name("candidates"));
            Field<Long> cParent = DSL.field(DSL.name("candidates", "parent"), Long.class);
            Field<Long> cChild = DSL.field(DSL.name("candidates", "child"), Long.class);
            try {
                context.mergeInto(NAMESPACE).using(context.selectOne()).on(NAMESPACE.NAME.eq(n))
                       .whenNotMatchedThenInsert(NAMESPACE.NAME).values(n).execute();
//            var namespace = context.select(NAMESPACE.ID).from(NAMESPACE).where(NAMESPACE.NAME.eq(n)).fetchOne()
//                                   .value1();

                context.mergeInto(SUBJECT).using(context.selectOne()).on(SUBJECT.NAME.eq(p))
                       .whenNotMatchedThenInsert(SUBJECT.NAME).values(p).execute();

                context.mergeInto(SUBJECT).using(context.selectOne()).on(SUBJECT.NAME.eq(c))
                       .whenNotMatchedThenInsert(SUBJECT.NAME).values(c).execute();

                var parent = context.select(SUBJECT.ID).from(SUBJECT).where(SUBJECT.NAME.eq(p)).fetchOne().value1();
                var child = context.select(SUBJECT.ID).from(SUBJECT).where(SUBJECT.NAME.eq(c)).fetchOne().value1();

                if (context.fetchExists(context.select(EDGE.ID).from(EDGE).where(EDGE.PARENT.eq(parent))
                                               .and(EDGE.CHILD.eq(child)).and(EDGE.HOPS.eq(0)))) {
                    return;
                }
                if (parent == child ||
                    context.fetchExists(context.select(EDGE.ID).from(EDGE).where(EDGE.PARENT.eq(parent))
                                               .and(EDGE.CHILD.eq(child)))) {
                    throw new SQLException(String.format("Cycle inserting: %s to: %s", parent, child));
                }

                context.createTemporaryTable(candidates).column(cParent).column(cChild).execute();

                var A = EDGE.as("A");
                var B = EDGE.as("B");
                context.insertInto(candidates)
                       .select(context.select(EDGE.PARENT, DSL.val(child)).from(EDGE).where(EDGE.CHILD.eq(parent))
                                      .union(context.select(DSL.val(parent), EDGE.CHILD).from(EDGE)
                                                    .where(EDGE.PARENT.endsWith(DSL.value(child))))
                                      .union(context.select(A.PARENT, B.CHILD).from(A.crossJoin(B))
                                                    .where(A.CHILD.eq(parent).and(B.PARENT.eq(child)))))
                       .execute();

                context.insertInto(EDGE).columns(EDGE.PARENT, EDGE.CHILD, EDGE.HOPS)
                       .values(DSL.value(parent), DSL.value(child), DSL.value(0)).execute();

                var E = EDGE.as("E");
                context.insertInto(EDGE).columns(EDGE.PARENT, EDGE.CHILD, EDGE.HOPS)
                       .select(context.select(cParent, cChild, DSL.val(1)).from(candidates)
                                      .whereNotExists(context.select(E.HOPS).from(E).where(E.PARENT.eq(cParent))
                                                             .and(E.CHILD.eq(cChild)).and(E.HOPS.eq(1))))
                       .execute();
            } finally {
                context.dropTable(candidates).execute();
            }
        });
    }

    public void delete(String namespace, String p, String c) {
//        dslCtx.transaction(ctx -> {
//            var context = DSL.using(ctx);
//            var resolved = context.select(OBJECT.ID, RELATION.ID, SUBJECT.ID).from(OBJECT, RELATION, SUBJECT)
//                                  .join(OBJECT).on(NAMESPACE.ID.eq(OBJECT.NAMESPACE)).where(OBJECT.NAME.eq(p))
//                                  .and(SUBJECT.NAME.eq(c)).fetchOne();
//            if (resolved == null) {
//                throw new SQLException(String.format("Edge parent: %s:%s child: %s does not exist", namespace, p, c));
//            }
//
//            var parent = resolved.value1();
//            var child = resolved.value3();
//
//            var idR = context.select(EDGE.ID).from(EDGE).where(EDGE.PARENT.eq(parent)).and(EDGE.CHILD.eq(child))
//                             .and(EDGE.HOPS.eq(0)).fetchOne();
//            if (idR == null) {
//                throw new SQLException(String.format("Edge parent: %s:%s child: %s does not exist", namespace, p, c));
//            }
//            var id = idR.value1();
//
//            // Temporary table to track the purged
//            Table<org.jooq.Record> purgeList = DSL.table(DSL.name("purge_list"));
//            Field<Long> pID = DSL.field(DSL.name("purge_list", "id"), Long.class);
//            context.createTemporaryTable(purgeList);
//
//            // Rows that were originally inserted with the first add() call for this direct
//            // edge
//            context.insertInto(purgeList).select(context.select(EDGE.ID).from(EDGE).where(EDGE.DIRECT.eq(id)))
//                   .execute();
//
//            // Scan and find all dependent rows that are inserted afterwards
//            while (true) {
//                if (context.insertInto(purgeList)
//                           .select(context.select(EDGE.ID).from(EDGE)
//                                          .where(EDGE.HOPS.gt(0)
//                                                          .and(EDGE.ENTRY.in(context.select(pID).from(purgeList))
//                                                                         .or(EDGE.EXIT.in(context.select(pID)
//                                                                                                 .from(purgeList)))))
//                                          .and(EDGE.ID.notIn(context.select(pID).from(purgeList))))
//                           .execute() == 0) {
//                    break;
//                }
//            }
//
//            context.delete(EDGE).where(EDGE.ID.in(context.select(pID).from(purgeList))).execute();
//            context.dropTable(purgeList);
//        });
    }

    private void dump(DSLContext context, String string) {
        var pa = SUBJECT.as("parent");
        var ch = SUBJECT.as("child");
        System.out.println(string);
        System.out.println(context.select(pa.NAME.as("parent"), ch.NAME.as("child"), EDGE.HOPS).from(pa, ch).join(EDGE)
                                  .on(EDGE.PARENT.eq(pa.ID).and(EDGE.CHILD.eq(ch.ID))).fetch());
        System.out.println();
        System.out.println();
    }
}
