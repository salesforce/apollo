/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import static com.salesforce.apollo.delphinius.schema.tables.Edge.EDGE;
import static com.salesforce.apollo.delphinius.schema.tables.Namespace.NAMESPACE;
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

    public enum Type {
        SUBJECT {
            @Override
            public String type() {
                return "s";
            }
        },
        RELATION {
            @Override
            public String type() {
                return "r";
            }
        },
        OBJECT {
            @Override
            public String type() {
                return "o";
            }
        };

        public abstract String type();
    }

    private final DSLContext dslCtx;

    public Oracle(Connection connection) {
        this(DSL.using(connection));
    }

    public Oracle(DSLContext dslCtx) {
        this.dslCtx = dslCtx;
    }

    public void addTuple(Type pType, Long parent, Type cType, Long child) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            Table<org.jooq.Record> candidates = DSL.table(DSL.name("candidates"));
            Field<String> cParentType = DSL.field(DSL.name("candidates", "parent_type"), String.class);
            Field<Long> cParent = DSL.field(DSL.name("candidates", "parent"), Long.class);
            Field<String> cChildType = DSL.field(DSL.name("candidates", "child_type"), String.class);
            Field<Long> cChild = DSL.field(DSL.name("candidates", "child"), Long.class);

            try {
                if (context.fetchExists(context.select(EDGE.ID).from(EDGE).where(EDGE.PARENT.eq(parent))
                                               .and(EDGE.CHILD.eq(child)).and(EDGE.HOPS.eq(0)))) {
                    return;
                }
                if (parent == child ||
                    context.fetchExists(context.select(EDGE.ID).from(EDGE).where(EDGE.PARENT.eq(parent))
                                               .and(EDGE.CHILD.eq(child)))) {
                    throw new SQLException(String.format("Cycle inserting: %s to: %s", parent, child));
                }

                context.createTemporaryTable(candidates).column(cParentType).column(cParent).column(cChildType)
                       .column(cChild).execute();

                var A = EDGE.as("A");
                var B = EDGE.as("B");
                context.insertInto(candidates)
                       .select(context.select(EDGE.PARENT_TYPE, EDGE.PARENT, DSL.val(cType.type()), DSL.val(child))
                                      .from(EDGE).where(EDGE.CHILD_TYPE.eq(pType.type())).and(EDGE.CHILD.eq(parent))
                                      .union(context.select(DSL.val(pType.type()), DSL.val(parent), EDGE.CHILD_TYPE,
                                                            EDGE.CHILD)
                                                    .from(EDGE).where(EDGE.PARENT_TYPE.eq(DSL.value(pType.type())))
                                                    .and(EDGE.PARENT.eq(DSL.value(child))))
                                      .union(context.select(A.PARENT_TYPE, A.PARENT, B.CHILD_TYPE, B.CHILD)
                                                    .from(A.crossJoin(B))
                                                    .where(A.CHILD_TYPE.eq(DSL.value(pType.type())))
                                                    .and(A.CHILD.eq(parent).and(B.PARENT_TYPE.eq(cType.type()))
                                                                .and(B.PARENT.eq(child)))))
                       .execute();

                context.insertInto(EDGE).columns(EDGE.PARENT_TYPE, EDGE.PARENT, EDGE.CHILD_TYPE, EDGE.CHILD, EDGE.HOPS)
                       .values(DSL.value(pType.type()), DSL.value(parent), DSL.value(cType.type()), DSL.value(child),
                               DSL.value(0))
                       .execute();

                var E = EDGE.as("E");
                context.insertInto(EDGE).columns(EDGE.PARENT_TYPE, EDGE.PARENT, EDGE.CHILD_TYPE, EDGE.CHILD, EDGE.HOPS)
                       .select(context.select(cParentType, cParent, cChildType, cChild, DSL.val(1)).from(candidates)
                                      .whereNotExists(context.select(E.HOPS).from(E)
                                                             .where(E.PARENT_TYPE.eq(cParentType))
                                                             .and(E.PARENT.eq(cParent)).and(E.CHILD_TYPE.eq(cChildType))
                                                             .and(E.CHILD.eq(cChild)).and(E.HOPS.eq(1))))
                       .execute();
            } finally {
                context.dropTable(candidates).execute();
            }
        });
    }

    public void addTuple(String n, String p, String c) throws SQLException {
        var context = dslCtx;
        context.mergeInto(NAMESPACE).using(context.selectOne()).on(NAMESPACE.NAME.eq(n))
               .whenNotMatchedThenInsert(NAMESPACE.NAME).values(n).execute();
//    var namespace = context.select(NAMESPACE.ID).from(NAMESPACE).where(NAMESPACE.NAME.eq(n)).fetchOne()
//                           .value1();

        context.mergeInto(SUBJECT).using(context.selectOne()).on(SUBJECT.NAME.eq(p))
               .whenNotMatchedThenInsert(SUBJECT.NAME).values(p).execute();

        context.mergeInto(SUBJECT).using(context.selectOne()).on(SUBJECT.NAME.eq(c))
               .whenNotMatchedThenInsert(SUBJECT.NAME).values(c).execute();

        var parent = context.select(SUBJECT.ID).from(SUBJECT).where(SUBJECT.NAME.eq(p)).fetchOne().value1();
        var child = context.select(SUBJECT.ID).from(SUBJECT).where(SUBJECT.NAME.eq(c)).fetchOne().value1();

        addTuple(Type.SUBJECT, parent, Type.SUBJECT, child);
    }

    public void delete(String namespace, String p, String c) {

    }

    @SuppressWarnings("unused")
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
