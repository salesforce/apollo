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
import static com.salesforce.apollo.delphinius.schema.tables.Tuple.TUPLE;

import java.sql.Connection;
import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.impl.DSL;

/**
 * An Access Control Oracle
 * 
 * @author hal.hildebrand
 *
 */
public class Oracle {

    public record Namespace(String name) {
        public Object object(String name) {
            return new Object(this, name);
        }

        public Relation relation(String name) {
            return new Relation(this, name);
        }

        public Subject subject(String name) {
            return new Subject(this, name);
        }
    }

    public record Subject(Namespace namespace, String name) {}

    public record Object(Namespace namespace, String name) {
        public Tuple tuple(Relation relation, Subject subject) {
            return new Tuple(this, relation, subject);
        }
    }

    public record Relation(Namespace namespace, String name) {}

    public record Tuple(Object object, Relation relation, Subject subject) {}

    private record NamespacedId(Long namespace, Long id) {}

    public static final String OBJECT_TYPE = "o";

    public static final String RELATION_TYPE = "r";

    public static final String SUBJECT_TYPE = "s";

    public static Namespace namespace(String name) {
        return new Namespace(name);
    }

    static void addEdge(DSLContext dslCtx, String type, Long parent, Long child) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            Table<org.jooq.Record> candidates = DSL.table(DSL.name("candidates"));
            Field<Long> cParent = DSL.field(DSL.name("candidates", "parent"), Long.class);
            Field<Long> cChild = DSL.field(DSL.name("candidates", "child"), Long.class);

            try {
                if (context.fetchExists(context.select(EDGE.ID).from(EDGE).where(EDGE.TYPE.eq(type))
                                               .and(EDGE.PARENT.eq(parent)).and(EDGE.CHILD.eq(child))
                                               .and(EDGE.HOPS.isFalse()))) {
                    return;
                }
                if (parent == child ||
                    context.fetchExists(context.select(EDGE.ID).from(EDGE).where(EDGE.TYPE.eq(type))
                                               .and(EDGE.PARENT.eq(parent)).and(EDGE.CHILD.eq(child)))) {
                    throw new SQLException(String.format("Cycle inserting: %s to: %s", parent, child));
                }

                context.createTemporaryTable(candidates).column(cParent).column(cChild).execute();

                var A = EDGE.as("A");
                var B = EDGE.as("B");
                context.insertInto(candidates)
                       .select(context.select(EDGE.PARENT, DSL.val(child)).from(EDGE).where(EDGE.CHILD.eq(parent))
                                      .and(EDGE.TYPE.eq(type))
                                      .union(context.select(DSL.val(parent), EDGE.CHILD).from(EDGE)
                                                    .where(EDGE.TYPE.eq(DSL.value(type)))
                                                    .and(EDGE.PARENT.eq(DSL.value(child))))
                                      .union(context.select(A.PARENT, B.CHILD).from(A.crossJoin(B))
                                                    .where(A.CHILD.eq(parent)).and(B.PARENT.eq(child))
                                                    .and(A.TYPE.eq(type)).and(B.TYPE.eq(type))))
                       .execute();

                context.insertInto(EDGE).columns(EDGE.TYPE, EDGE.PARENT, EDGE.CHILD, EDGE.HOPS)
                       .values(DSL.value(type), DSL.value(parent), DSL.value(child), DSL.value(false)).execute();

                var E = EDGE.as("E");
                context.insertInto(EDGE).columns(EDGE.TYPE, EDGE.PARENT, EDGE.CHILD, EDGE.HOPS)
                       .select(context.select(DSL.val(type), cParent, cChild, DSL.val(true)).from(candidates)
                                      .whereNotExists(context.select(E.HOPS).from(E).where(E.PARENT.eq(cParent))
                                                             .and(E.CHILD.eq(cChild)).and(E.HOPS.isTrue())))
                       .execute();
            } finally {
                context.dropTable(candidates).execute();
            }
        });
    }

    static void deleteEdge(DSLContext c, String type, Long parent, Long child) throws SQLException {
        c.transaction(ctx -> {
            var context = DSL.using(ctx);
            if (context.deleteFrom(EDGE).where(EDGE.HOPS.isFalse()).and(EDGE.TYPE.eq(type).and(EDGE.PARENT.eq(parent)))
                       .and(EDGE.CHILD.eq(child)).execute() == 0) {
                return; // Does not exist
            }
            var A = EDGE.as("A");
            var B = EDGE.as("B");

            Field<Long> sParent = DSL.field(DSL.name("C", "parent"), Long.class);
            Field<Long> sChild = DSL.field(DSL.name("C", "child"), Long.class);

            context.update(EDGE).set(EDGE.DEL_MARK, true).where(EDGE.HOPS.isTrue())
                   .and(EDGE.ID.in(context.select(EDGE.ID).from(EDGE)
                                          .join(context.select(EDGE.PARENT, DSL.val(child).as(EDGE.CHILD)).from(EDGE)
                                                       .where(EDGE.CHILD.eq(parent))

                                                       .union(context.select(DSL.val(parent), EDGE.CHILD.as(EDGE.CHILD))
                                                                     .from(EDGE).where(EDGE.PARENT.eq(child)))

                                                       .union(context.select(A.PARENT, B.CHILD).from(A).crossJoin(B)
                                                                     .where(A.CHILD.eq(parent)).and(B.PARENT.eq(child)))
                                                       .asTable("C"))
                                          .on(sParent.eq(EDGE.PARENT)).and(sChild.eq(EDGE.CHILD))))
                   .execute();

            var ROWZ = DSL.name("rowz");
            Table<Record> rowzTable = DSL.table(ROWZ);
            Table<Record> s1 = rowzTable.as("S1");
            Field<Long> s1Parent = DSL.field(DSL.name("S1", "parent"), Long.class);
            Field<Long> s1Child = DSL.field(DSL.name("S1", "child"), Long.class);
            Table<Record> s2 = rowzTable.as("S2");
            Field<Long> s2Parent = DSL.field(DSL.name("S2", "parent"), Long.class);
            Field<Long> s2Child = DSL.field(DSL.name("S2", "child"), Long.class);
            Table<Record> s3 = rowzTable.as("S3");
            Field<Long> s3Parent = DSL.field(DSL.name("S3", "parent"), Long.class);
            Field<Long> s3Child = DSL.field(DSL.name("S3", "child"), Long.class);

            context.with(ROWZ).as(context.select(EDGE.PARENT, EDGE.CHILD).from(EDGE).where(EDGE.DEL_MARK.isFalse()))
                   .update(EDGE).set(EDGE.DEL_MARK, DSL.val(false)).where(EDGE.DEL_MARK.isTrue())
                   .and(EDGE.ID.in(context.select(EDGE.ID).from(EDGE).innerJoin(s1).on(s1Parent.eq(EDGE.PARENT))
                                          .innerJoin(s2).on(s1Child.eq(s2Parent)).and(s2Child.eq(EDGE.CHILD))))
                   .execute();

            context.with(ROWZ).as(context.select(EDGE.PARENT, EDGE.CHILD).from(EDGE).where(EDGE.DEL_MARK.isFalse()))
                   .update(EDGE).set(EDGE.DEL_MARK, DSL.val(false)).where(EDGE.DEL_MARK.isTrue())
                   .and(EDGE.ID.in(context.select(EDGE.ID).from(EDGE).innerJoin(s1).on(s1Parent.eq(EDGE.PARENT))
                                          .innerJoin(s2).on(s1Child.eq(s2Parent)).innerJoin(s3).on(s2Child.eq(s3Parent))
                                          .and(s3Child.eq(EDGE.CHILD))))
                   .execute();

            context.deleteFrom(EDGE).where(EDGE.DEL_MARK.isTrue()).execute();
        });
    }

    static SelectJoinStep<?> grants(DSLContext ctx, Long o, Long r, Long s) throws SQLException {
        var ACL = TUPLE.as("ACL");
        Table<Record1<Long>> subject = ctx.select(EDGE.CHILD.as("subject_id")).from(EDGE)
                                          .where(EDGE.TYPE.eq(SUBJECT_TYPE)).and(EDGE.PARENT.eq(s))
                                          .union(ctx.select(DSL.val(s).as("subject_id"))).asTable();
        Field<Long> subjectId = subject.field("subject_id", Long.class);

        Table<Record1<Long>> relation = ctx.select(EDGE.CHILD.as("relation_id")).from(EDGE)
                                           .where(EDGE.TYPE.eq(RELATION_TYPE)).and(EDGE.PARENT.eq(r))
                                           .union(DSL.select(DSL.val(r).as("relation_id"))).asTable();
        Field<Long> relationId = relation.field("relation_id", Long.class);

        Table<Record1<Long>> object = ctx.select(EDGE.CHILD.as("object_id")).from(EDGE).where(EDGE.TYPE.eq(OBJECT_TYPE))
                                         .and(EDGE.PARENT.eq(o)).union(DSL.select(DSL.val(o).as("object_id")))
                                         .asTable();
        Field<Long> objectId = object.field("object_id", Long.class);

        return ctx.select(subjectId, relationId, objectId)
                  .from(subject.crossJoin(relation).crossJoin(object).innerJoin(ACL)
                               .on(subjectId.eq(ACL.SUBJECT)
                                            .and(relationId.eq(ACL.RELATION).and(objectId.eq(ACL.RELATION)))));
    }

    private final DSLContext dslCtx;

    public Oracle(Connection connection) {
        this(DSL.using(connection));
    }

    public Oracle(DSLContext dslCtx) {
        this.dslCtx = dslCtx;
    }

    public void add(Namespace namespace) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);

            context.mergeInto(NAMESPACE).using(context.selectOne()).on(NAMESPACE.NAME.eq(namespace.name))
                   .whenNotMatchedThenInsert(NAMESPACE.NAME).values(namespace.name).execute();
        });
    }

    public void add(Object object) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.mergeInto(NAMESPACE).using(context.selectOne()).on(NAMESPACE.NAME.eq(object.namespace.name))
                   .whenNotMatchedThenInsert(NAMESPACE.NAME).values(object.namespace.name).execute();
            var namespace = context.select(NAMESPACE.ID).from(NAMESPACE).where(NAMESPACE.NAME.eq(object.namespace.name))
                                   .fetchOne().value1();

            context.mergeInto(OBJECT).using(context.selectOne()).on(OBJECT.NAMESPACE.eq(namespace))
                   .and(OBJECT.NAME.eq(object.name)).whenNotMatchedThenInsert(OBJECT.NAMESPACE, OBJECT.NAME)
                   .values(namespace, object.name).execute();
        });
    }

    public void add(Relation relation) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.mergeInto(NAMESPACE).using(context.selectOne()).on(NAMESPACE.NAME.eq(relation.namespace.name))
                   .whenNotMatchedThenInsert(NAMESPACE.NAME).values(relation.namespace.name).execute();
            var namespace = context.select(NAMESPACE.ID).from(NAMESPACE)
                                   .where(NAMESPACE.NAME.eq(relation.namespace.name)).fetchOne().value1();

            context.mergeInto(RELATION).using(context.selectOne()).on(RELATION.NAMESPACE.eq(namespace))
                   .and(RELATION.NAME.eq(relation.name)).whenNotMatchedThenInsert(RELATION.NAMESPACE, RELATION.NAME)
                   .values(namespace, relation.name).execute();
        });
    }

    public void add(Subject subject) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.mergeInto(NAMESPACE).using(context.selectOne()).on(NAMESPACE.NAME.eq(subject.namespace.name))
                   .whenNotMatchedThenInsert(NAMESPACE.NAME).values(subject.namespace.name).execute();
            var namespace = context.select(NAMESPACE.ID).from(NAMESPACE)
                                   .where(NAMESPACE.NAME.eq(subject.namespace.name)).fetchOne().value1();

            context.mergeInto(SUBJECT).using(context.selectOne()).on(SUBJECT.NAMESPACE.eq(namespace))
                   .and(SUBJECT.NAME.eq(subject.name)).whenNotMatchedThenInsert(SUBJECT.NAMESPACE, SUBJECT.NAME)
                   .values(namespace, subject.name).execute();
        });
    }

    public void add(Tuple tuple) throws SQLException {
        var s = resolve(tuple.subject, true);
        var r = resolve(tuple.relation, true);
        var o = resolve(tuple.object, true);
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.mergeInto(TUPLE).using(context.selectOne()).on(TUPLE.SUBJECT.eq(s.id)).and(TUPLE.OBJECT.eq(o.id))
                   .and(TUPLE.RELATION.eq(r.id)).whenNotMatchedThenInsert(TUPLE.OBJECT, TUPLE.RELATION, TUPLE.SUBJECT)
                   .values(o.id, r.id, s.id).execute();
        });
    }

    public boolean check(Tuple tuple) throws SQLException {
        var s = resolve(tuple.subject, false);
        var r = resolve(tuple.relation, false);
        var o = resolve(tuple.object, false);
        if (s == null || r == null || o == null) {
            return false;
        }
        return dslCtx.fetchExists(dslCtx.selectOne().from(grants(dslCtx, o.id, r.id, s.id)));
    }

    public void delete(Object object) {
    }

    public void delete(Relation relation) {
    }

    public void delete(Subject subject) {
    }

    public void delete(Tuple tuple) throws SQLException {
        var s = resolve(tuple.subject, false);
        var r = resolve(tuple.relation, false);
        var o = resolve(tuple.object, false);
        if (s == null || r == null || o == null) {
            return;
        }
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.deleteFrom(TUPLE).where(TUPLE.OBJECT.eq(o.id)).and(TUPLE.RELATION.eq(r.id))
                   .and(TUPLE.SUBJECT.eq(s.id)).execute();
        });
    }

    public void map(Object parent, Object child) throws SQLException {
        addEdge(dslCtx, OBJECT_TYPE, resolve(parent, true).id, resolve(child, true).id);
    }

    public void map(Relation parent, Relation child) throws SQLException {
        addEdge(dslCtx, RELATION_TYPE, resolve(parent, true).id, resolve(child, true).id);
    }

    public void map(Subject parent, Subject child) throws SQLException {
        addEdge(dslCtx, SUBJECT_TYPE, resolve(parent, true).id, resolve(child, true).id);
    }

    public void remove(Object parent, Object child) throws SQLException {
        NamespacedId a = resolve(parent, false);
        if (a == null) {
            return;
        }
        NamespacedId b = resolve(child, false);
        if (b == null) {
            return;
        }
        deleteEdge(dslCtx, OBJECT_TYPE, a.id, b.id);
    }

    public void remove(Relation parent, Relation child) throws SQLException {
        NamespacedId a = resolve(parent, false);
        if (a == null) {
            return;
        }
        NamespacedId b = resolve(child, false);
        if (b == null) {
            return;
        }
        deleteEdge(dslCtx, RELATION_TYPE, a.id, b.id);
    }

    public void remove(Subject parent, Subject child) throws SQLException {
        NamespacedId a = resolve(parent, false);
        if (a == null) {
            return;
        }
        NamespacedId b = resolve(child, false);
        if (b == null) {
            return;
        }
        deleteEdge(dslCtx, SUBJECT_TYPE, a.id, b.id);
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

    private NamespacedId resolve(Object object, boolean add) throws SQLException {
        if (add) {
            add(object);
        }
        var namespace = dslCtx.select(NAMESPACE.ID).from(NAMESPACE).where(NAMESPACE.NAME.eq(object.namespace.name))
                              .fetchOne();
        if (!add && namespace == null) {
            return null;
        }
        var id = dslCtx.select(OBJECT.ID).from(OBJECT)
                       .where(OBJECT.NAMESPACE.eq(namespace.value1()).and(OBJECT.NAME.eq(object.name))).fetchOne();
        if (!add && id == null) {
            return null;
        }
        return new NamespacedId(namespace.value1(), id.value1());
    }

    private NamespacedId resolve(Relation relation, boolean add) throws SQLException {
        if (add) {
            add(relation);
        }
        var namespace = dslCtx.select(NAMESPACE.ID).from(NAMESPACE).where(NAMESPACE.NAME.eq(relation.namespace.name))
                              .fetchOne();
        if (!add && namespace == null) {
            return null;
        }
        var id = dslCtx.select(RELATION.ID).from(RELATION)
                       .where(RELATION.NAMESPACE.eq(namespace.value1()).and(RELATION.NAME.eq(relation.name)))
                       .fetchOne();
        if (!add && id == null) {
            return null;
        }
        return new NamespacedId(namespace.value1(), id.value1());
    }

    private NamespacedId resolve(Subject subject, boolean add) throws SQLException {
        if (add) {
            add(subject);
        }
        var namespace = dslCtx.select(NAMESPACE.ID).from(NAMESPACE).where(NAMESPACE.NAME.eq(subject.namespace.name))
                              .fetchOne();
        if (!add && namespace == null) {
            return null;
        }
        var id = dslCtx.select(SUBJECT.ID).from(SUBJECT).where(SUBJECT.NAMESPACE.eq(namespace.value1()))
                       .and(SUBJECT.NAME.eq(subject.name)).fetchOne();
        if (!add && id == null) {
            return null;
        }
        return new NamespacedId(namespace.value1(), id.value1());
    }
}
