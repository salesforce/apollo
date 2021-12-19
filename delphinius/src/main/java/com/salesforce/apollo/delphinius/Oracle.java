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

    public void addTuple(String n, String p, String r, String c) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.mergeInto(NAMESPACE).using(context.selectOne()).on(NAMESPACE.NAME.eq(n))
                   .whenNotMatchedThenInsert(NAMESPACE.NAME).values(n).execute();
            var namespace = context.select(NAMESPACE.ID).from(NAMESPACE).where(NAMESPACE.NAME.eq(n)).fetchOne()
                                   .value1();

            context.mergeInto(SUBJECT).using(context.selectOne()).on(SUBJECT.NAME.eq(p))
                   .whenNotMatchedThenInsert(SUBJECT.NAME).values(p).execute();

            context.mergeInto(RELATION).using(context.selectOne()).on(RELATION.NAMESPACE.eq(namespace))
                   .and(RELATION.NAME.eq(r)).whenNotMatchedThenInsert(RELATION.NAMESPACE, RELATION.NAME)
                   .values(namespace, r).execute();

            context.mergeInto(SUBJECT).using(context.selectOne()).on(SUBJECT.NAME.eq(c))
                   .whenNotMatchedThenInsert(SUBJECT.NAME).values(c).execute();

            var parent = context.select(SUBJECT.ID).from(SUBJECT).where(SUBJECT.NAME.eq(p)).fetchOne().value1();
            var relation = context.select(RELATION.ID).from(RELATION).where(RELATION.NAMESPACE.eq(namespace))
                                  .and(RELATION.NAME.eq(r)).fetchOne().value1();
            var child = context.select(SUBJECT.ID).from(SUBJECT).where(SUBJECT.NAME.eq(c)).fetchOne().value1();

            if (parent == child || context.fetchExists(context.select(EDGE.ID).from(EDGE).where(EDGE.PARENT.eq(parent))
                                                              .and(EDGE.CHILD.eq(child)).and(EDGE.HOPS.eq(0)))) {
                return;
            }
            if (context.fetchExists(context.select(EDGE.ID).from(EDGE).where(EDGE.PARENT.eq(parent))
                                           .and(EDGE.CHILD.eq(child)))) {
                throw new SQLException(String.format("Cycle inserting: %s rel: %s to: %s", parent, relation, child));
            }
            var id = context.insertInto(EDGE).columns(EDGE.PARENT, EDGE.RELATION, EDGE.CHILD, EDGE.HOPS)
                            .values(parent, relation, child, 0).returning().fetchOne().getId();

            context.update(EDGE).set(EDGE.ENTRY, id).set(EDGE.EXIT, id).set(EDGE.DIRECT, id).where(EDGE.ID.eq(id))
                   .execute();

            dump(context, String.format("Initial insert: %s-%s-%s", p, r, c));

            // step 1: parent's incoming edges to child
            var edgeAsChild = EDGE.as("child");
            context.insertInto(EDGE)
                   .columns(EDGE.ENTRY, EDGE.DIRECT, EDGE.EXIT, EDGE.PARENT, EDGE.RELATION, EDGE.CHILD, EDGE.HOPS)
                   .select(context.select(edgeAsChild.ID, DSL.val(id), DSL.val(id), edgeAsChild.PARENT,
                                          edgeAsChild.RELATION, DSL.val(child), edgeAsChild.HOPS.plus(1))
                                  .from(edgeAsChild).where(edgeAsChild.CHILD.eq(parent))
                                  .and(edgeAsChild.RELATION.eq(relation)))
                   .execute();

            dump(context, String.format("Parent incoming to child: %s-%s-%s", p, r, c));

            // step 2: parent to child's outgoing edges
            context.insertInto(EDGE)
                   .columns(EDGE.ENTRY, EDGE.DIRECT, EDGE.EXIT, EDGE.PARENT, EDGE.RELATION, EDGE.CHILD, EDGE.HOPS)
                   .select(context.select(DSL.val(id), DSL.val(id), EDGE.EXIT, DSL.val(parent), EDGE.RELATION,
                                          EDGE.CHILD, EDGE.HOPS.plus(1))
                                  .from(EDGE).where(EDGE.PARENT.eq(child).and(EDGE.RELATION.eq(relation))))
                   .execute();

            dump(context, String.format("Parent to child's outgoing: %s-%s-%s", p, r, c));

            // step 3: parent's incoming edges to end vertex of child's outgoing edges
            var A = EDGE.as("parent");
            var B = EDGE.as("child");
            context.insertInto(EDGE)
                   .columns(EDGE.ENTRY, EDGE.DIRECT, EDGE.EXIT, EDGE.PARENT, EDGE.RELATION, EDGE.CHILD, EDGE.HOPS)
                   .select(context.select(A.ENTRY, DSL.val(id), B.EXIT, A.PARENT, A.RELATION, B.CHILD,
                                          A.HOPS.plus(B.HOPS).plus(2))
                                  .from(A).crossJoin(B).where(A.CHILD.eq(parent)).and(A.RELATION.eq(relation))
                                  .and(B.PARENT.eq(child)).and(B.RELATION.eq(relation)))
                   .execute();
            dump(context, String.format("Parent's incoming to end vertex of child's outgoing: %s-%s-%s", p, r, c));
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

            var idR = context.select(EDGE.ID).from(EDGE).where(EDGE.PARENT.eq(parent))
                             .and(EDGE.RELATION.eq(relation).and(EDGE.CHILD.eq(child)).and(EDGE.HOPS.eq(0))).fetchOne();
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

    private void dump(DSLContext context, String string) {
        var pa = SUBJECT.as("parent");
        var ch = SUBJECT.as("child");
        System.out.println(string);
        System.out.println(context.select(pa.NAME.as("parent"), RELATION.NAME.as("relation"), ch.NAME.as("child"),
                                          EDGE.HOPS)
                                  .from(pa, RELATION, ch).join(EDGE)
                                  .on(EDGE.PARENT.eq(pa.ID).and(EDGE.RELATION.eq(RELATION.ID))
                                                 .and(EDGE.CHILD.eq(ch.ID)))
                                  .fetch());
        System.out.println();
        System.out.println();
    }
}
