/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import com.salesforce.apollo.delphinius.schema.tables.Edge;
import org.jooq.Record;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.salesforce.apollo.delphinius.schema.tables.Assertion.ASSERTION;
import static com.salesforce.apollo.delphinius.schema.tables.Edge.EDGE;
import static com.salesforce.apollo.delphinius.schema.tables.Namespace.NAMESPACE;
import static com.salesforce.apollo.delphinius.schema.tables.Object.OBJECT;
import static com.salesforce.apollo.delphinius.schema.tables.Relation.RELATION;
import static com.salesforce.apollo.delphinius.schema.tables.Subject.SUBJECT;

/**
 * An Access Control Oracle
 *
 * @author hal.hildebrand
 */
abstract public class AbstractOracle implements Oracle {

    protected static final Edge                   A          = EDGE.as("A");
    protected static final Edge                   B          = EDGE.as("B");
    protected static final Table<org.jooq.Record> candidates = DSL.table(DSL.name("CANDIDATES"));
    protected static final Field<Long>            cChild     = DSL.field(DSL.name("CANDIDATES", "CHILD"), Long.class);
    protected static final Field<Long>            cParent    = DSL.field(DSL.name("CANDIDATES", "PARENT"), Long.class);
    protected static final Edge                   E          = EDGE.as("E");
    protected static final Logger                 log        = LoggerFactory.getLogger(AbstractOracle.class);
    protected static final Name                   ROWZ       = DSL.name("ROWZ");
    protected static final Table<Record>          rowzTable  = DSL.table(ROWZ);
    protected static final Table<Record>          s1         = rowzTable.as("S1");
    protected static final Table<Record>          s2         = rowzTable.as("S2");
    protected static final Table<Record>          s3         = rowzTable.as("S3");
    protected static final Field<Long>            s1Child    = DSL.field(DSL.name("S1", "CHILD"), Long.class);
    protected static final Field<Long>            s1Parent   = DSL.field(DSL.name("S1", "PARENT"), Long.class);
    protected static final Field<Long>            s2Child    = DSL.field(DSL.name("S2", "CHILD"), Long.class);
    protected static final Field<Long>            s2Parent   = DSL.field(DSL.name("S2", "PARENT"), Long.class);
    protected static final Field<Long>            s3Child    = DSL.field(DSL.name("S3", "CHILD"), Long.class);
    protected static final Field<Long>            s3Parent   = DSL.field(DSL.name("S3", "PARENT"), Long.class);
    protected static final Field<Long>            sChild     = DSL.field(DSL.name("SUSPECT", "CHILD"), Long.class);
    protected static final Field<Long>            sParent    = DSL.field(DSL.name("SUSPECT", "PARENT"), Long.class);
    protected static final Name                   suspect    = DSL.name("SUSPECT");
    private final          DSLContext             dslCtx;

    public AbstractOracle(Connection connection) {
        this(DSL.using(connection, SQLDialect.H2));
    }

    public AbstractOracle(DSLContext dslCtx) {
        this.dslCtx = dslCtx;
    }

    public static boolean addAssertion(Connection connection, String subjectNamespace, String subjectName,
                                       String subjectRelationNamespace, String subjectRelationName,
                                       String objectNamespace, String objectName, String objectRelationNamespace,
                                       String objectRelationName) throws SQLException {
        var subject = new Subject(new Namespace(subjectNamespace), subjectName,
                                  new Relation(new Namespace(subjectRelationNamespace), subjectRelationName));
        var object = new Object(new Namespace(objectNamespace), objectName,
                                new Relation(new Namespace(objectRelationNamespace), objectRelationName));
        var assertion = subject.assertion(object);

        return add(DSL.using(connection, SQLDialect.H2), assertion);
    }

    public static void addNamespace(Connection connection, String name) throws SQLException {
        var namespace = new Namespace(name);

        add(DSL.using(connection, SQLDialect.H2), namespace);
    }

    public static void addObject(Connection connection, String objectNamespace, String objectName,
                                 String objectRelationNamespace, String objectRelationName) throws SQLException {
        var object = new Object(new Namespace(objectNamespace), objectName,
                                new Relation(new Namespace(objectRelationNamespace), objectRelationName));

        add(DSL.using(connection, SQLDialect.H2), object);
    }

    public static void addRelation(Connection connection, String relationNamespace, String relationName)
    throws SQLException {
        var relation = new Relation(new Namespace(relationNamespace), relationName);

        add(DSL.using(connection, SQLDialect.H2), relation);
    }

    public static void addSubject(Connection connection, String subjectNamespace, String subjectName,
                                  String subjectRelationNamespace, String subjectRelationName) throws SQLException {
        var subject = new Subject(new Namespace(subjectNamespace), subjectName,
                                  new Relation(new Namespace(subjectRelationNamespace), subjectRelationName));

        add(DSL.using(connection, SQLDialect.H2), subject);
    }

    public static void deleteAssertion(Connection connection, String subjectNamespace, String subjectName,
                                       String subjectRelationNamespace, String subjectRelationName,
                                       String objectNamespace, String objectName, String objectRelationNamespace,
                                       String objectRelationName) throws SQLException {
        var subject = new Subject(new Namespace(subjectNamespace), subjectName,
                                  new Relation(new Namespace(subjectRelationNamespace), subjectRelationName));
        var object = new Object(new Namespace(objectNamespace), objectName,
                                new Relation(new Namespace(objectRelationNamespace), objectRelationName));
        var assertion = subject.assertion(object);

        delete(DSL.using(connection, SQLDialect.H2), assertion);
    }

    public static void deleteNamespace(Connection connection, String name) throws SQLException {
        var namespace = new Namespace(name);

        delete(DSL.using(connection, SQLDialect.H2), namespace);
    }

    public static void deleteObject(Connection connection, String objectNamespace, String objectName,
                                    String objectRelationNamespace, String objectRelationName) throws SQLException {
        var object = new Object(new Namespace(objectNamespace), objectName,
                                new Relation(new Namespace(objectRelationNamespace), objectRelationName));

        delete(DSL.using(connection, SQLDialect.H2), object);
    }

    public static void deleteRelation(Connection connection, String relationNamespace, String relationName)
    throws SQLException {
        var relation = new Relation(new Namespace(relationNamespace), relationName);

        delete(DSL.using(connection, SQLDialect.H2), relation);
    }

    public static void deleteSubject(Connection connection, String subjectNamespace, String subjectName,
                                     String subjectRelationNamespace, String subjectRelationName) throws SQLException {
        var subject = new Subject(new Namespace(subjectNamespace), subjectName,
                                  new Relation(new Namespace(subjectRelationNamespace), subjectRelationName));

        delete(DSL.using(connection, SQLDialect.H2), subject);
    }

    public static void mapObject(Connection connection, String parentNamespace, String parentName,
                                 String parentRelationNamespace, String parentRelationName, String childNamespace,
                                 String childName, String childRelationNamespace, String childRelationName)
    throws SQLException {
        var parent = new Object(new Namespace(parentNamespace), parentName,
                                new Relation(new Namespace(parentRelationNamespace), parentRelationName));
        var child = new Object(new Namespace(childNamespace), childName,
                               new Relation(new Namespace(childRelationNamespace), childRelationName));

        map(parent, DSL.using(connection, SQLDialect.H2), child);
    }

    public static void mapRelation(Connection connection, String parentNamespace, String parentName,
                                   String childNamespace, String childName) throws SQLException {
        var parent = new Relation(new Namespace(parentNamespace), parentName);
        var child = new Relation(new Namespace(childNamespace), childName);

        map(parent, DSL.using(connection, SQLDialect.H2), child);
    }

    public static void mapSubject(Connection connection, String parentNamespace, String parentName,
                                  String parentRelationNamespace, String parentRelationName, String childNamespace,
                                  String childName, String childRelationNamespace, String childRelationName)
    throws SQLException {
        var parent = new Subject(new Namespace(parentNamespace), parentName,
                                 new Relation(new Namespace(parentRelationNamespace), parentRelationName));
        var child = new Subject(new Namespace(childNamespace), childName,
                                new Relation(new Namespace(childRelationNamespace), childRelationName));

        map(parent, DSL.using(connection, SQLDialect.H2), child);
    }

    public static void removeObject(Connection connection, String parentNamespace, String parentName,
                                    String parentRelationNamespace, String parentRelationName, String childNamespace,
                                    String childName, String childRelationNamespace, String childRelationName)
    throws SQLException {
        var parent = new Object(new Namespace(parentNamespace), parentName,
                                new Relation(new Namespace(parentRelationNamespace), parentRelationName));
        var child = new Object(new Namespace(childNamespace), childName,
                               new Relation(new Namespace(childRelationNamespace), childRelationName));

        remove(parent, DSL.using(connection, SQLDialect.H2), child);
    }

    public static void removeRelation(Connection connection, String parentNamespace, String parentName,
                                      String childNamespace, String childName) throws SQLException {
        var parent = new Relation(new Namespace(parentNamespace), parentName);
        var child = new Relation(new Namespace(childNamespace), childName);

        remove(parent, DSL.using(connection, SQLDialect.H2), child);
    }

    public static void removeSubject(Connection connection, String parentNamespace, String parentName,
                                     String parentRelationNamespace, String parentRelationName, String childNamespace,
                                     String childName, String childRelationNamespace, String childRelationName)
    throws SQLException {
        var parent = new Subject(new Namespace(parentNamespace), parentName,
                                 new Relation(new Namespace(parentRelationNamespace), parentRelationName));
        var child = new Subject(new Namespace(childNamespace), childName,
                                new Relation(new Namespace(childRelationNamespace), childRelationName));

        remove(parent, DSL.using(connection, SQLDialect.H2), child);
    }

    public static boolean add(DSLContext context, Assertion assertion) throws SQLException {
        var s = resolveAdd(context, assertion.subject());
        var o = resolveAdd(context, assertion.object());
        return addAssert(context, s.id(), o.id());
    }

    public static boolean addAssert(DSLContext context, long s, long o) {
        return context.mergeInto(ASSERTION)
                      .using(context.selectOne())
                      .on(ASSERTION.SUBJECT.eq(s))
                      .and(ASSERTION.OBJECT.eq(o))
                      .whenNotMatchedThenInsert(ASSERTION.OBJECT, ASSERTION.SUBJECT)
                      .values(o, s)
                      .execute() != 0;
    }

    public static void add(DSLContext context, Namespace namespace) {
        context.mergeInto(NAMESPACE)
               .using(context.selectOne())
               .on(NAMESPACE.NAME.eq(namespace.name()))
               .whenNotMatchedThenInsert(NAMESPACE.NAME)
               .values(namespace.name())
               .execute();
    }

    public static void add(DSLContext context, Object object) throws SQLException {
        var relation = resolveAdd(context, object.relation());
        var namespace = resolveAdd(context, object.namespace());
        var name = object.name();
        addObj(context, namespace, name, relation);
    }

    public static void addObj(DSLContext context, Long namespace, String name, NamespacedId relation) {
        context.mergeInto(OBJECT)
               .using(context.selectOne())
               .on(OBJECT.NAMESPACE.eq(namespace))
               .and(OBJECT.NAME.eq(name))
               .and(OBJECT.RELATION.eq(relation.id()))
               .whenNotMatchedThenInsert(OBJECT.NAMESPACE, OBJECT.NAME, OBJECT.RELATION)
               .values(namespace, name, relation.id())
               .execute();
    }

    public static void add(DSLContext context, Relation relation) throws SQLException {
        var namespace = resolveAdd(context, relation.namespace());
        var name = relation.name();
        addRel(context, namespace, name);
    }

    public static void addRel(DSLContext context, Long namespace, String name) {
        context.mergeInto(RELATION)
               .using(context.selectOne())
               .on(RELATION.NAMESPACE.eq(namespace))
               .and(RELATION.NAME.eq(name))
               .whenNotMatchedThenInsert(RELATION.NAMESPACE, RELATION.NAME)
               .values(namespace, name)
               .execute();
    }

    public static void add(DSLContext context, Subject subject) throws SQLException {
        var namespace = resolveAdd(context, subject.namespace());
        var relation = resolveAdd(context, subject.relation());
        var name = subject.name();
        addSubj(context, namespace, name, relation);
    }

    public static void addSubj(DSLContext context, Long namespace, String name, NamespacedId relation) {
        context.mergeInto(SUBJECT)
               .using(context.selectOne())
               .on(SUBJECT.NAMESPACE.eq(namespace))
               .and(SUBJECT.NAME.eq(name))
               .and(SUBJECT.RELATION.eq(relation.id()))
               .whenNotMatchedThenInsert(SUBJECT.NAMESPACE, SUBJECT.NAME, SUBJECT.RELATION)
               .values(namespace, name, relation.id())
               .execute();
    }

    public static void addEdge(DSLContext context, Long parent, String type, Long child) throws SQLException {
        try {
            if (context.fetchExists(context.select(EDGE.ID)
                                           .from(EDGE)
                                           .where(EDGE.TYPE.eq(type))
                                           .and(EDGE.PARENT.eq(parent))
                                           .and(EDGE.CHILD.eq(child))
                                           .and(EDGE.TRANSITIVE.isFalse()))) {
                return;
            }
            if (parent == child || context.fetchExists(context.select(EDGE.ID)
                                                              .from(EDGE)
                                                              .where(EDGE.TYPE.eq(type))
                                                              .and(EDGE.PARENT.eq(parent))
                                                              .and(EDGE.CHILD.eq(child)))) {
                throw new SQLException(String.format("Cycle inserting: %s to: %s", parent, child));
            }

            context.createTemporaryTable(candidates).column(cParent).column(cChild).execute();

            // Candidates via direct transitive edges
            context.insertInto(candidates)
                   .select(context.select(EDGE.PARENT, DSL.val(child))
                                  .from(EDGE)
                                  .where(EDGE.CHILD.eq(parent))
                                  .and(EDGE.TYPE.eq(type))
                                  .union(context.select(DSL.val(parent), EDGE.CHILD)
                                                .from(EDGE)
                                                .where(EDGE.TYPE.eq(DSL.value(type)))
                                                .and(EDGE.PARENT.eq(DSL.value(child))))
                                  .union(context.select(A.PARENT, B.CHILD)
                                                .from(A.crossJoin(B))
                                                .where(A.CHILD.eq(parent))
                                                .and(B.PARENT.eq(child))
                                                .and(A.TYPE.eq(type))
                                                .and(B.TYPE.eq(type))))
                   .execute();
            // Direct edge
            context.insertInto(EDGE)
                   .columns(EDGE.TYPE, EDGE.PARENT, EDGE.CHILD, EDGE.TRANSITIVE)
                   .values(DSL.value(type), DSL.value(parent), DSL.value(child), DSL.value(false))
                   .execute();

            // Transitive edges
            context.insertInto(EDGE)
                   .columns(EDGE.TYPE, EDGE.PARENT, EDGE.CHILD, EDGE.TRANSITIVE)
                   .select(context.select(DSL.val(type), cParent, cChild, DSL.val(true))
                                  .from(candidates)
                                  .whereNotExists(context.select(E.TRANSITIVE)
                                                         .from(E)
                                                         .where(E.PARENT.eq(cParent))
                                                         .and(E.CHILD.eq(cChild))
                                                         .and(E.TRANSITIVE.isTrue())))
                   .execute();
        } finally {
            try {
                context.dropTable(candidates).execute();
            } catch (DataAccessException e) {
                // ignored
            }
        }
    }

    static void delete(DSLContext context, Assertion assertion) throws SQLException {
        var s = resolve(context, assertion.subject());
        var o = resolve(context, assertion.object());
        if (s == null || o == null) {
            return;
        }
        context.deleteFrom(ASSERTION).where(ASSERTION.OBJECT.eq(o.id())).and(ASSERTION.SUBJECT.eq(s.id())).execute();
    }

    static void delete(DSLContext context, Namespace namespace) throws SQLException {
        var resolved = resolve(context, namespace);
        if (resolved == null) {
            return;
        }
        context.deleteFrom(NAMESPACE).where(NAMESPACE.ID.eq(resolved)).execute();
    }

    static void delete(DSLContext context, Object object) throws SQLException {
        var resolved = resolve(context, object);
        if (resolved == null) {
            return;
        }
        context.deleteFrom(OBJECT).where(OBJECT.ID.eq(resolved.id())).execute();
    }

    static void delete(DSLContext context, Relation relation) throws SQLException {
        var resolved = resolve(context, relation);
        if (resolved == null) {
            return;
        }
        context.deleteFrom(RELATION).where(RELATION.ID.eq(resolved.id())).execute();
    }

    static void delete(DSLContext context, Subject subject) throws SQLException {
        var resolved = resolve(context, subject);
        if (resolved == null) {
            return;
        }
        context.deleteFrom(SUBJECT).where(SUBJECT.ID.eq(resolved.id())).execute();
    }

    static void deleteEdge(DSLContext c, Long parent, String type, Long child) throws SQLException {
        c.transaction(ctx -> {
            var context = DSL.using(ctx);
            if (context.deleteFrom(EDGE)
                       .where(EDGE.TRANSITIVE.isFalse())
                       .and(EDGE.TYPE.eq(type).and(EDGE.PARENT.eq(parent)))
                       .and(EDGE.CHILD.eq(child))
                       .execute() == 0) {
                return; // Does not exist
            }

            context.update(EDGE)
                   .set(EDGE.MARK, true)
                   .where(EDGE.ID.in(context.select(EDGE.ID)
                                            .from(EDGE)
                                            .join(context.select(EDGE.PARENT, DSL.val(child).as(EDGE.CHILD))
                                                         .from(EDGE)
                                                         .where(EDGE.CHILD.eq(parent))

                                                         .union(
                                                         context.select(DSL.val(parent), EDGE.CHILD.as(EDGE.CHILD))
                                                                .from(EDGE)
                                                                .where(EDGE.PARENT.eq(child)))

                                                         .union(context.select(A.PARENT, B.CHILD)
                                                                       .from(A)
                                                                       .crossJoin(B)
                                                                       .where(A.CHILD.eq(parent))
                                                                       .and(B.PARENT.eq(child)))
                                                         .asTable(suspect))
                                            .on(sParent.eq(EDGE.PARENT))
                                            .and(sChild.eq(EDGE.CHILD))).and(EDGE.TRANSITIVE.isTrue()))
                   .execute();

            context.with(ROWZ)
                   .as(context.select(EDGE.PARENT, EDGE.CHILD).from(EDGE).where(EDGE.MARK.isFalse()))
                   .update(EDGE)
                   .set(EDGE.MARK, DSL.val(false))
                   .where(EDGE.ID.in(context.select(EDGE.ID)
                                            .from(EDGE)
                                            .innerJoin(s1)
                                            .on(s1Parent.eq(EDGE.PARENT))
                                            .innerJoin(s2)
                                            .on(s1Child.eq(s2Parent))
                                            .and(s2Child.eq(EDGE.CHILD))))
                   .and(EDGE.MARK.isTrue())
                   .execute();

            context.with(ROWZ)
                   .as(context.select(EDGE.PARENT, EDGE.CHILD).from(EDGE).where(EDGE.MARK.isFalse()))
                   .update(EDGE)
                   .set(EDGE.MARK, DSL.val(false))
                   .where(EDGE.ID.in(context.select(EDGE.ID)
                                            .from(EDGE)
                                            .innerJoin(s1)
                                            .on(s1Parent.eq(EDGE.PARENT))
                                            .innerJoin(s2)
                                            .on(s1Child.eq(s2Parent))
                                            .innerJoin(s3)
                                            .on(s2Child.eq(s3Parent))
                                            .and(s3Child.eq(EDGE.CHILD))))
                   .and(EDGE.MARK.isTrue())
                   .execute();

            context.deleteFrom(EDGE).where(EDGE.MARK.isTrue()).execute();
        });
    }

    static SelectJoinStep<Record2<Long, Long>> grants(Long s, DSLContext ctx, Long o) throws SQLException {
        var subject = ctx.select(EDGE.CHILD.as("SUBJECT_ID"))
                         .from(EDGE)
                         .where(EDGE.TYPE.eq(SUBJECT_TYPE))
                         .and(EDGE.PARENT.eq(s))
                         .union(ctx.select(DSL.val(s).as("SUBJECT_ID")))
                         .asTable();
        var subjectId = subject.field("SUBJECT_ID", Long.class);

        var object = ctx.select(EDGE.CHILD.as("OBJECT_ID"))
                        .from(EDGE)
                        .where(EDGE.TYPE.eq(OBJECT_TYPE))
                        .and(EDGE.PARENT.eq(o))
                        .union(DSL.select(DSL.val(o).as("OBJECT_ID")))
                        .asTable();

        var objectId = object.field("OBJECT_ID", Long.class);

        return ctx.select(subjectId, objectId)
                  .from(subject.crossJoin(object)
                               .innerJoin(ASSERTION)
                               .on(subjectId.eq(ASSERTION.SUBJECT).and(objectId.eq(ASSERTION.OBJECT))));
    }

    static void map(Object parent, DSLContext context, Object child) throws SQLException {
        addEdge(context, resolveAdd(context, parent).id(), OBJECT_TYPE, resolveAdd(context, child).id());
    }

    static void map(Relation parent, DSLContext context, Relation child) throws SQLException {
        addEdge(context, resolveAdd(context, parent).id(), RELATION_TYPE, resolveAdd(context, child).id());
    }

    static void map(Subject parent, DSLContext context, Subject child) throws SQLException {
        addEdge(context, resolveAdd(context, parent).id(), SUBJECT_TYPE, resolveAdd(context, child).id());
    }

    static void remove(Object parent, DSLContext context, Object child) throws SQLException {
        var a = resolve(context, parent);
        if (a == null) {
            return;
        }
        var b = resolve(context, child);
        if (b == null) {
            return;
        }
        deleteEdge(context, a.id(), OBJECT_TYPE, b.id());
    }

    static void remove(Relation parent, DSLContext context, Relation child) throws SQLException {
        var a = resolve(context, parent);
        if (a == null) {
            return;
        }
        var b = resolve(context, child);
        if (b == null) {
            return;
        }
        deleteEdge(context, a.id(), RELATION_TYPE, b.id());
    }

    static void remove(Subject parent, DSLContext context, Subject child) throws SQLException {
        var a = resolve(context, parent);
        if (a == null) {
            return;
        }
        var b = resolve(context, child);
        if (b == null) {
            return;
        }
        deleteEdge(context, a.id(), SUBJECT_TYPE, b.id());
    }

    public static Long resolve(DSLContext context, Namespace namespace) throws SQLException {
        var resolved = context.select(NAMESPACE.ID)
                              .from(NAMESPACE)
                              .where(NAMESPACE.NAME.eq(namespace.name()))
                              .fetchOne();
        if (resolved == null) {
            return null;
        }
        return resolved.value1();
    }

    public static NamespacedId resolve(DSLContext context, Object object) throws SQLException {
        var namespace = resolve(context, object.namespace());
        if (namespace == null) {
            return null;
        }
        var relation = resolve(context, object.relation());
        if (relation == null) {
            return null;
        }

        var rel = relation.id();
        var name = object.name();
        var resolved = resolveObj(context, namespace, name, rel);
        if (resolved == null) {
            return null;
        }
        return new NamespacedId(namespace, resolved.value1(), rel);
    }

    public static Record1<Long> resolveObj(DSLContext context, Long namespace, String name, Long rel) {
        return context.select(OBJECT.ID)
                      .from(OBJECT)
                      .where(OBJECT.NAMESPACE.eq(namespace).and(OBJECT.NAME.eq(name)).and(OBJECT.RELATION.eq(rel)))
                      .fetchOne();
    }

    public static NamespacedId resolve(DSLContext context, Relation relation) throws SQLException {
        var namespace = resolve(context, relation.namespace());
        if (namespace == null) {
            return null;
        }
        var name = relation.name();
        var resolved = resolveRel(context, namespace, name);
        if (resolved == null) {
            return null;
        }
        return new NamespacedId(namespace, resolved.value1(), 0L);
    }

    public static Record1<Long> resolveRel(DSLContext context, Long namespace, String name) {
        return context.select(RELATION.ID)
                      .from(RELATION)
                      .where(RELATION.NAMESPACE.eq(namespace).and(RELATION.NAME.eq(name)))
                      .fetchOne();
    }

    public static NamespacedId resolve(DSLContext context, Subject subject) throws SQLException {
        var namespace = resolve(context, subject.namespace());
        if (namespace == null) {
            return null;
        }
        var relation = resolve(context, subject.relation());
        if (relation == null) {
            return null;
        }
        var name = subject.name();
        var rel = relation.id();
        var resolved = resolveSubj(context, namespace, name, rel);
        if (resolved == null) {
            return null;
        }
        return new NamespacedId(namespace, resolved.value1(), rel);
    }

    public static Record1<Long> resolveSubj(DSLContext context, Long namespace, String name, Long rel) {
        return context.select(SUBJECT.ID)
                      .from(SUBJECT)
                      .where(SUBJECT.NAMESPACE.eq(namespace))
                      .and(SUBJECT.NAME.eq(name))
                      .and(SUBJECT.RELATION.eq(rel))
                      .fetchOne();
    }

    public static Long resolveAdd(DSLContext context, Namespace namespace) throws SQLException {
        add(context, namespace);
        var resolved = context.select(NAMESPACE.ID)
                              .from(NAMESPACE)
                              .where(NAMESPACE.NAME.eq(namespace.name()))
                              .fetchOne();
        if (resolved == null) {
            return null;
        }
        return resolved.value1();
    }

    public static NamespacedId resolveAdd(DSLContext context, Object object) throws SQLException {
        add(context, object);
        var namespace = resolveAdd(context, object.namespace());
        if (namespace == null) {
            return null;
        }
        var relation = resolveAdd(context, object.relation());
        if (relation == null) {
            return null;
        }

        var resolved = context.select(OBJECT.ID)
                              .from(OBJECT)
                              .where(OBJECT.NAMESPACE.eq(namespace)
                                                     .and(OBJECT.NAME.eq(object.name()))
                                                     .and(OBJECT.RELATION.eq(relation.id())))
                              .fetchOne();
        if (resolved == null) {
            return null;
        }
        return new NamespacedId(namespace, resolved.value1(), relation.id());
    }

    public static NamespacedId resolveAdd(DSLContext context, Relation relation) throws SQLException {
        add(context, relation);
        var namespace = resolveAdd(context, relation.namespace());
        if (namespace == null) {
            return null;
        }
        var resolved = context.select(RELATION.ID)
                              .from(RELATION)
                              .where(RELATION.NAMESPACE.eq(namespace).and(RELATION.NAME.eq(relation.name())))
                              .fetchOne();
        if (resolved == null) {
            return null;
        }
        return new NamespacedId(namespace, resolved.value1(), 0L);
    }

    public static NamespacedId resolveAdd(DSLContext context, Subject subject) throws SQLException {
        add(context, subject);
        var namespace = resolveAdd(context, subject.namespace());
        if (namespace == null) {
            return null;
        }
        var relation = resolveAdd(context, subject.relation());
        if (relation == null) {
            return null;
        }
        var resolved = context.select(SUBJECT.ID)
                              .from(SUBJECT)
                              .where(SUBJECT.NAMESPACE.eq(namespace))
                              .and(SUBJECT.NAME.eq(subject.name()))
                              .and(SUBJECT.RELATION.eq(relation.id()))
                              .fetchOne();
        if (resolved == null) {
            return null;
        }
        return new NamespacedId(namespace, resolved.value1(), relation.id());
    }

    /**
     * Check the assertion.
     *
     * @return true if the assertion is made, false if not
     */
    public boolean check(Assertion assertion) throws SQLException {
        var s = resolve(dslCtx, assertion.subject());
        var o = resolve(dslCtx, assertion.object());
        if (s == null || o == null) {
            return false;
        }
        return dslCtx.fetchExists(dslCtx.selectOne().from(grants(s.id(), dslCtx, o.id())));
    }

    /**
     * Answer the list of direct and transitive Subjects that map to the supplied object. The query only considers
     * subjects with assertions that match the object completely - i.e. {namespace, name, relation}
     *
     * @throws SQLException
     */
    public List<Subject> expand(Object object) throws SQLException {
        return subjects(null, object).toList();
    }

    /**
     * Answer the list of direct and transitive Subjects that map to the object from subjects that have the supplied
     * predicate as their relation. The query only considers assertions that match the object completely - i.e.
     * {namespace, name, relation}
     *
     * @throws SQLException
     */
    public List<Subject> expand(Relation predicate, Object object) throws SQLException {
        return subjects(predicate, object).toList();
    }

    /**
     * Answer the list of direct and transitive Objects that map to the subject from objects that have the supplied
     * predicate as their relation. The query only considers assertions that match the subject completely - i.e.
     * {namespace, name, relation}
     *
     * @throws SQLException
     */
    @Override
    public List<Object> expand(Relation predicate, Subject subject) throws SQLException {
        return objects(predicate, subject).toList();
    }

    /**
     * Answer the list of direct and transitive Objects that map to the supplied subject. The query only considers
     * objects with assertions that match the subject completely - i.e. {namespace, name, relation}
     *
     * @throws SQLException
     */
    @Override
    public List<Object> expand(Subject subject) throws SQLException {
        return objects(null, subject).toList();
    }

    /**
     * Answer the list of direct Subjects that map to the supplied objects. The query only considers subjects with
     * assertions that match the objects completely - i.e. {namespace, name, relation}
     *
     * @throws SQLException
     */
    public List<Subject> read(Object... objects) throws SQLException {
        return Arrays.asList(objects).stream().flatMap(object -> {
            try {
                return directSubjects(null, object);
            } catch (SQLException e) {
                log.error("error getting direct subjects of: {}", object, e);
                return null;
            }
        }).filter(o -> o != null).toList();
    }

    /**
     * Answer the list of direct Subjects that map to the supplied objects. The query only considers subjects with
     * assertions that match the objects completely - i.e. {namespace, name, relation} and only the subjects that have
     * the matching predicate
     *
     * @throws SQLException
     */
    public List<Subject> read(Relation predicate, Object... objects) throws SQLException {
        return Arrays.asList(objects).stream().flatMap(object -> {
            try {
                return directSubjects(predicate, object);
            } catch (SQLException e) {
                log.error("error getting direct subjects (#{}) of: {}", predicate, object, e);
                return null;
            }
        }).filter(s -> s != null).toList();
    }

    /**
     * Answer the list of direct Objects that map to the supplied subjects. The query only considers objects with
     * assertions that match the subjects completely - i.e. {namespace, name, relation} and only the objects that have
     * the matching predicate
     *
     * @throws SQLException
     */
    @Override
    public List<Object> read(Relation predicate, Subject... subjects) throws SQLException {
        return Arrays.asList(subjects).stream().flatMap(subject -> {
            try {
                return directObjects(predicate, subject);
            } catch (SQLException e) {
                log.error("error getting direct objects (#{}) of: {}", predicate, subject, e);
                return null;
            }
        }).filter(o -> o != null).toList();
    }

    /**
     * Answer the list of direct Objects that map to the supplied subjects. The query only considers objects with
     * assertions that match the subjects completely - i.e. {namespace, name, relation}
     *
     * @throws SQLException
     */
    @Override
    public List<Object> read(Subject... subjects) throws SQLException {
        return Arrays.asList(subjects).stream().flatMap(subject -> {
            try {
                return directObjects(null, subject);
            } catch (SQLException e) {
                log.error("error getting direct objects of: {}", subject, e);
                return null;
            }
        }).filter(s -> s != null).toList();
    }

    /**
     * Answer the list of direct and transitive subjects that map to the object. These subjects may be further filtered
     * by the predicate Relation, if not null. The query only considers assertions that match the object completely -
     * i.e. {namespace, name, relation}
     *
     * @throws SQLException
     */
    @Override
    public Stream<Subject> subjects(Relation predicate, Object object) throws SQLException {
        var resolved = resolve(dslCtx, object);
        if (resolved == null) {
            return Stream.empty();
        }

        NamespacedId relation = null;
        if (predicate != null) {
            relation = resolve(dslCtx, predicate);
            if (relation == null) {
                return Stream.empty();
            }
        }

        var subject = dslCtx.select(EDGE.PARENT.as("INFERRED"), EDGE.CHILD.as("DIRECT"))
                            .from(EDGE)
                            .where(EDGE.TYPE.eq(SUBJECT_TYPE))
                            .asTable("S");

        var direct = subject.field("DIRECT", Long.class);
        var inferred = subject.field("INFERRED", Long.class);

        var o = dslCtx.select(EDGE.CHILD.as("OBJECT_ID"))
                      .from(EDGE)
                      .where(EDGE.TYPE.eq(OBJECT_TYPE))
                      .and(EDGE.PARENT.eq(resolved.id()))
                      .union(DSL.select(DSL.val(resolved.id()).as("OBJECT_ID")))
                      .asTable();
        var objectId = o.field("OBJECT_ID", Long.class);

        var relNs = NAMESPACE.as("REL_NS");
        var subNs = NAMESPACE.as("SUB_NS");

        var base = dslCtx.selectDistinct(subNs.NAME, SUBJECT.NAME, relNs.NAME, RELATION.NAME)
                         .from(SUBJECT)
                         .join(subNs)
                         .on(subNs.ID.eq(SUBJECT.NAMESPACE))
                         .join(RELATION)
                         .on(RELATION.ID.eq(SUBJECT.RELATION))
                         .join(relNs)
                         .on(relNs.ID.eq(RELATION.NAMESPACE))
                         .join(dslCtx.select(inferred, direct)
                                     .from(subject.crossJoin(o)
                                                  .innerJoin(ASSERTION)
                                                  .on(direct.eq(ASSERTION.SUBJECT).or(inferred.eq(ASSERTION.SUBJECT)))
                                                  .and(objectId.eq(ASSERTION.OBJECT)))
                                     .asTable("S"))
                         .on(SUBJECT.ID.eq(direct))
                         .or(SUBJECT.ID.eq(inferred));
        var query = relation == null ? base : base.where(SUBJECT.RELATION.eq(relation.id()));
        return query.stream()
                    .map(r -> new Subject(new Namespace(r.value1()), r.value2(),
                                          new Relation(new Namespace(r.value3()), r.value4())));
    }

    private Stream<Object> directObjects(Relation predicate, Subject subject) throws SQLException {
        var resolved = resolve(dslCtx, subject);
        if (resolved == null) {
            return Stream.empty();
        }

        NamespacedId relation = null;
        if (predicate != null) {
            relation = resolve(dslCtx, predicate);
            if (relation == null) {
                return Stream.empty();
            }
        }
        var relNs = NAMESPACE.as("REL_NS");
        var objNs = NAMESPACE.as("OBJ_NS");
        var query = dslCtx.selectDistinct(objNs.NAME, OBJECT.NAME, relNs.NAME, RELATION.NAME)
                          .from(OBJECT)
                          .join(objNs)
                          .on(objNs.ID.eq(OBJECT.NAMESPACE))
                          .join(RELATION)
                          .on(RELATION.ID.eq(OBJECT.RELATION))
                          .join(relNs)
                          .on(relNs.ID.eq(RELATION.NAMESPACE))
                          .join(ASSERTION)
                          .on(OBJECT.ID.eq(ASSERTION.OBJECT))
                          .and(ASSERTION.SUBJECT.eq(resolved.id()));
        if (relation != null) {
            query = query.and(OBJECT.RELATION.eq(relation.id()));
        }
        return query.stream()
                    .map(r -> new Object(new Namespace(r.value1()), r.value2(),
                                         new Relation(new Namespace(r.value3()), r.value4())));
    }

    private Stream<Subject> directSubjects(Relation predicate, Object object) throws SQLException {
        var resolved = resolve(dslCtx, object);
        if (resolved == null) {
            return Stream.empty();
        }

        NamespacedId relation = null;
        if (predicate != null) {
            relation = resolve(dslCtx, predicate);
            if (relation == null) {
                return Stream.empty();
            }
        }
        var relNs = NAMESPACE.as("REL_NS");
        var subNs = NAMESPACE.as("SUB_NS");
        var query = dslCtx.selectDistinct(subNs.NAME, SUBJECT.NAME, relNs.NAME, RELATION.NAME)
                          .from(SUBJECT)
                          .join(subNs)
                          .on(subNs.ID.eq(SUBJECT.NAMESPACE))
                          .join(RELATION)
                          .on(RELATION.ID.eq(SUBJECT.RELATION))
                          .join(relNs)
                          .on(relNs.ID.eq(RELATION.NAMESPACE))
                          .join(ASSERTION)
                          .on(SUBJECT.ID.eq(ASSERTION.SUBJECT))
                          .and(ASSERTION.OBJECT.eq(resolved.id()));
        if (relation != null) {
            query = query.and(SUBJECT.RELATION.eq(relation.id()));
        }
        return query.stream()
                    .map(r -> new Subject(new Namespace(r.value1()), r.value2(),
                                          new Relation(new Namespace(r.value3()), r.value4())));
    }

    /**
     * Answer the list of direct and transitive objects that map to the subject. These object may further filtered by
     * the predicate Relation, if not null. The query only considers assertions that match the subject completely - i.e.
     * {namespace, name, relation}
     *
     * @throws SQLException
     */
    private Stream<Object> objects(Relation predicate, Subject subject) throws SQLException {
        var resolved = resolve(dslCtx, subject);
        if (resolved == null) {
            return Stream.empty();
        }

        NamespacedId relation = null;
        if (predicate != null) {
            relation = resolve(dslCtx, predicate);
            if (relation == null) {
                return Stream.empty();
            }
        }

        return Stream.empty(); // my brain hurts too much currently to construct the sql
    }
}
