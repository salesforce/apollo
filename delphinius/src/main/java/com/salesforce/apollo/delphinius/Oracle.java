/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import static com.salesforce.apollo.delphinius.schema.tables.Assertion.ASSERTION;
import static com.salesforce.apollo.delphinius.schema.tables.Edge.EDGE;
import static com.salesforce.apollo.delphinius.schema.tables.Namespace.NAMESPACE;
import static com.salesforce.apollo.delphinius.schema.tables.Object.OBJECT;
import static com.salesforce.apollo.delphinius.schema.tables.Relation.RELATION;
import static com.salesforce.apollo.delphinius.schema.tables.Subject.SUBJECT;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.delphinius.schema.tables.Edge;

/**
 * An Access Control Oracle
 * 
 * @author hal.hildebrand
 *
 */
public class Oracle {
    /** A Namespace **/
    public record Namespace(String name) {

        public Object object(String name, Relation relation) {
            return new Object(this, name, relation);
        }

        public Relation relation(String name) {
            return new Relation(this, name);
        }

        public Subject subject(String name) {
            return new Subject(this, name, NO_RELATION);
        }

        public Subject subject(String name, Relation relation) {
            return new Subject(this, name, relation);
        }
    }

    /** A Subject **/
    public record Subject(Namespace namespace, String name, Relation relation) {
        public Assertion assertion(Object object) {
            return new Assertion(this, object);
        }

        @Override
        public String toString() {
            return namespace.name + ":" + name + (relation.equals(NO_RELATION) ? "" : "#" + relation);
        }
    }

    /** An Object **/
    public record Object(Namespace namespace, String name, Relation relation) {
        public Assertion assertion(Subject subject) {
            return new Assertion(subject, this);
        }

        @Override
        public String toString() {
            return namespace.name + ":" + name + (relation.equals(NO_RELATION) ? "" : "#" + relation);
        }
    }

    /** A Relation **/
    public record Relation(Namespace namespace, String name) {
        @Override
        public String toString() {
            return namespace.name + ":" + name;
        }
    }

    /** An Assertion **/
    public record Assertion(Subject subject, Object object) {
        @Override
        public String toString() {
            return subject + "@" + object;
        }
    }

    private record NamespacedId(Long namespace, Long id, Long relation) {}

    /** Grounding for all the domains */
    public static final Assertion NO_ASSERTION;
    public static final Namespace NO_NAMESPACE;
    public static final Object    NO_OBJECT;
    public static final Relation  NO_RELATION;
    public static final Subject   NO_SUBJECT;

    static final String OBJECT_TYPE   = "o";
    static final String RELATION_TYPE = "r";
    static final String SUBJECT_TYPE  = "s";

    private static final Edge                   A          = EDGE.as("A");
    private static final Edge                   B          = EDGE.as("B");
    private static final Table<org.jooq.Record> candidates = DSL.table(DSL.name("candidates"));
    private static final Field<Long>            cChild     = DSL.field(DSL.name("candidates", "child"), Long.class);
    private static final Field<Long>            cParent    = DSL.field(DSL.name("candidates", "parent"), Long.class);
    private static final Edge                   E          = EDGE.as("E");
    private static final Logger                 log        = LoggerFactory.getLogger(Oracle.class);
    private static final Name                   ROWZ       = DSL.name("rowz");
    private static final Table<Record>          rowzTable  = DSL.table(ROWZ);
    private static final Table<Record>          s1         = rowzTable.as("S1");
    private static final Field<Long>            s1Child    = DSL.field(DSL.name("S1", "child"), Long.class);
    private static final Field<Long>            s1Parent   = DSL.field(DSL.name("S1", "parent"), Long.class);
    private static final Table<Record>          s2         = rowzTable.as("S2");
    private static final Field<Long>            s2Child    = DSL.field(DSL.name("S2", "child"), Long.class);
    private static final Field<Long>            s2Parent   = DSL.field(DSL.name("S2", "parent"), Long.class);
    private static final Table<Record>          s3         = rowzTable.as("S3");
    private static final Field<Long>            s3Child    = DSL.field(DSL.name("S3", "child"), Long.class);
    private static final Field<Long>            s3Parent   = DSL.field(DSL.name("S3", "parent"), Long.class);
    private static final Field<Long>            sChild     = DSL.field(DSL.name("suspect", "child"), Long.class);
    private static final Field<Long>            sParent    = DSL.field(DSL.name("suspect", "parent"), Long.class);
    private static final Name                   suspect    = DSL.name("suspect");

    static {
        NO_NAMESPACE = new Namespace("");
        NO_RELATION = new Relation(NO_NAMESPACE, "");
        NO_SUBJECT = new Subject(NO_NAMESPACE, "", NO_RELATION);
        NO_OBJECT = new Object(NO_NAMESPACE, "", NO_RELATION);
        NO_ASSERTION = new Assertion(NO_SUBJECT, NO_OBJECT);
    }

    public static Namespace namespace(String name) {
        return new Namespace(name);
    }

    static void addEdge(DSLContext dslCtx, Long parent, String type, Long child) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
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
                context.dropTable(candidates).execute();
            }
        });
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

                                                         .union(context.select(DSL.val(parent),
                                                                               EDGE.CHILD.as(EDGE.CHILD))
                                                                       .from(EDGE)
                                                                       .where(EDGE.PARENT.eq(child)))

                                                         .union(context.select(A.PARENT, B.CHILD)
                                                                       .from(A)
                                                                       .crossJoin(B)
                                                                       .where(A.CHILD.eq(parent))
                                                                       .and(B.PARENT.eq(child)))
                                                         .asTable(suspect))
                                            .on(sParent.eq(EDGE.PARENT))
                                            .and(sChild.eq(EDGE.CHILD)))
                                 .and(EDGE.TRANSITIVE.isTrue()))
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
        Table<Record1<Long>> subject = ctx.select(EDGE.CHILD.as("subject_id"))
                                          .from(EDGE)
                                          .where(EDGE.TYPE.eq(SUBJECT_TYPE))
                                          .and(EDGE.PARENT.eq(s))
                                          .union(ctx.select(DSL.val(s).as("subject_id")))
                                          .asTable();
        Field<Long> subjectId = subject.field("subject_id", Long.class);

        Table<Record1<Long>> object = ctx.select(EDGE.CHILD.as("object_id"))
                                         .from(EDGE)
                                         .where(EDGE.TYPE.eq(OBJECT_TYPE))
                                         .and(EDGE.PARENT.eq(o))
                                         .union(DSL.select(DSL.val(o).as("object_id")))
                                         .asTable();

        Field<Long> objectId = object.field("object_id", Long.class);

        return ctx.select(subjectId, objectId)
                  .from(subject.crossJoin(object)
                               .innerJoin(ASSERTION)
                               .on(subjectId.eq(ASSERTION.SUBJECT).and(objectId.eq(ASSERTION.OBJECT))));
    }

    private final DSLContext dslCtx;

    public Oracle(Connection connection) {
        this(DSL.using(connection));
    }

    public Oracle(DSLContext dslCtx) {
        this.dslCtx = dslCtx;
    }

    /**
     * Add an Assertion. The subject and object of the assertion will also be added
     * if they do not exist
     */
    public void add(Assertion assertion) throws SQLException {
        var s = resolve(assertion.subject, true);
        var o = resolve(assertion.object, true);
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.mergeInto(ASSERTION)
                   .using(context.selectOne())
                   .on(ASSERTION.SUBJECT.eq(s.id))
                   .and(ASSERTION.OBJECT.eq(o.id))
                   .whenNotMatchedThenInsert(ASSERTION.OBJECT, ASSERTION.SUBJECT)
                   .values(o.id, s.id)
                   .execute();
        });
    }

    /**
     * Add a Namespace.
     */
    public void add(Namespace namespace) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);

            context.mergeInto(NAMESPACE)
                   .using(context.selectOne())
                   .on(NAMESPACE.NAME.eq(namespace.name))
                   .whenNotMatchedThenInsert(NAMESPACE.NAME)
                   .values(namespace.name)
                   .execute();
        });
    }

    /**
     * Add an Object.
     */
    public void add(Object object) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            var relation = resolve(object.relation, true);
            var namespace = resolve(object.namespace, true);

            context.mergeInto(OBJECT)
                   .using(context.selectOne())
                   .on(OBJECT.NAMESPACE.eq(namespace))
                   .and(OBJECT.NAME.eq(object.name))
                   .and(OBJECT.RELATION.eq(relation.id))
                   .whenNotMatchedThenInsert(OBJECT.NAMESPACE, OBJECT.NAME, OBJECT.RELATION)
                   .values(namespace, object.name, relation.id)
                   .execute();
        });
    }

    /**
     * Add a Relation
     */
    public void add(Relation relation) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            var namespace = resolve(relation.namespace, true);

            context.mergeInto(RELATION)
                   .using(context.selectOne())
                   .on(RELATION.NAMESPACE.eq(namespace))
                   .and(RELATION.NAME.eq(relation.name))
                   .whenNotMatchedThenInsert(RELATION.NAMESPACE, RELATION.NAME)
                   .values(namespace, relation.name)
                   .execute();
        });
    }

    /**
     * Add a Subject
     */
    public void add(Subject subject) throws SQLException {
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            var namespace = resolve(subject.namespace, true);
            var relation = resolve(subject.relation, true);

            context.mergeInto(SUBJECT)
                   .using(context.selectOne())
                   .on(SUBJECT.NAMESPACE.eq(namespace))
                   .and(SUBJECT.NAME.eq(subject.name))
                   .and(SUBJECT.RELATION.eq(relation.id))
                   .whenNotMatchedThenInsert(SUBJECT.NAMESPACE, SUBJECT.NAME, SUBJECT.RELATION)
                   .values(namespace, subject.name, relation.id)
                   .execute();
        });
    }

    /**
     * Check the assertion.
     * 
     * @return true if the assertion is made, false if not
     */
    public boolean check(Assertion assertion) throws SQLException {
        var s = resolve(assertion.subject, false);
        var o = resolve(assertion.object, false);
        if (s == null || o == null) {
            return false;
        }
        return dslCtx.fetchExists(dslCtx.selectOne().from(grants(s.id, dslCtx, o.id)));
    }

    /**
     * Delete an assertion. Only the assertion is deleted, not the subject nor
     * object of the assertion.
     */
    public void delete(Assertion assertion) throws SQLException {
        var s = resolve(assertion.subject, false);
        var o = resolve(assertion.object, false);
        if (s == null || o == null) {
            return;
        }
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.deleteFrom(ASSERTION).where(ASSERTION.OBJECT.eq(o.id)).and(ASSERTION.SUBJECT.eq(s.id)).execute();
        });
    }

    /**
     * Delete an Object. All dependant uses of the object (mappings, Assertions) are
     * removed as well.
     */
    public void delete(Object object) throws SQLException {
        var resolved = resolve(object, false);
        if (resolved == null) {
            return;
        }
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.deleteFrom(OBJECT).where(OBJECT.ID.eq(resolved.id)).execute();
        });
    }

    /**
     * Delete an Relation. All dependant uses of the relation (mappings, Subject,
     * Object and Assertions) are removed as well.
     */
    public void delete(Relation relation) throws SQLException {
        var resolved = resolve(relation, false);
        if (resolved == null) {
            return;
        }
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.deleteFrom(RELATION).where(RELATION.ID.eq(resolved.id)).execute();
        });
    }

    /**
     * Delete an Subject. All dependant uses of the subject (mappings and
     * Assertions) are removed as well.
     */
    public void delete(Subject subject) throws SQLException {
        var resolved = resolve(subject, false);
        if (resolved == null) {
            return;
        }
        dslCtx.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.deleteFrom(SUBJECT).where(SUBJECT.ID.eq(resolved.id)).execute();
        });
    }

    /**
     * Answer the list of Subjects, both direct and transitive Subjects, that map to
     * the supplied object. The query only considers subjects with assertions that
     * match the object completely - i.e. {namespace, name, relation}
     * 
     * @throws SQLException
     */
    public List<Subject> expand(Object object) throws SQLException {
        return subjects(null, object).toList();
    }

    /**
     * Answer the list of Subjects, both direct and transitive, that map to the
     * object from subjects that have the supplied predicate as their relation. The
     * query only considers assertions that match the object completely - i.e.
     * {namespace, name, relation}
     * 
     * @throws SQLException
     */
    public List<Subject> expand(Relation predicate, Object object) throws SQLException {
        return subjects(predicate, object).toList();
    }

    /**
     * Map the parent object to the child
     */
    public void map(Object parent, Object child) throws SQLException {
        addEdge(dslCtx, resolve(parent, true).id, OBJECT_TYPE, resolve(child, true).id);
    }

    /**
     * Map the parent relation to the child
     */
    public void map(Relation parent, Relation child) throws SQLException {
        addEdge(dslCtx, resolve(parent, true).id, RELATION_TYPE, resolve(child, true).id);
    }

    /**
     * Map the parent subject to the child
     */
    public void map(Subject parent, Subject child) throws SQLException {
        addEdge(dslCtx, resolve(parent, true).id, SUBJECT_TYPE, resolve(child, true).id);
    }

    /**
     * Answer the list of direct Subjects that map to the supplied objects. The
     * query only considers subjects with assertions that match the objects
     * completely - i.e. {namespace, name, relation}
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
        }).filter(s -> s != null).toList();
    }

    /**
     * Answer the list of direct Subjects that map to the supplied objects. The
     * query only considers subjects with assertions that match the objects
     * completely - i.e. {namespace, name, relation} and only the subjects that have
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
     * Remove the mapping between the parent and the child objects
     */
    public void remove(Object parent, Object child) throws SQLException {
        NamespacedId a = resolve(parent, false);
        if (a == null) {
            return;
        }
        NamespacedId b = resolve(child, false);
        if (b == null) {
            return;
        }
        deleteEdge(dslCtx, a.id, OBJECT_TYPE, b.id);
    }

    /**
     * Remove the mapping between the parent and the child relations
     */
    public void remove(Relation parent, Relation child) throws SQLException {
        NamespacedId a = resolve(parent, false);
        if (a == null) {
            return;
        }
        NamespacedId b = resolve(child, false);
        if (b == null) {
            return;
        }
        deleteEdge(dslCtx, a.id, RELATION_TYPE, b.id);
    }

    /**
     * Remove the mapping between the parent and the child subects
     */
    public void remove(Subject parent, Subject child) throws SQLException {
        NamespacedId a = resolve(parent, false);
        if (a == null) {
            return;
        }
        NamespacedId b = resolve(child, false);
        if (b == null) {
            return;
        }
        deleteEdge(dslCtx, a.id, SUBJECT_TYPE, b.id);
    }

    private Stream<Subject> directSubjects(Relation predicate, Object object) throws SQLException {
        var resolved = resolve(object, false);
        if (resolved == null) {
            return Stream.empty();
        }

        NamespacedId relation = null;
        if (predicate != null) {
            relation = resolve(predicate, false);
            if (relation == null) {
                return Stream.empty();
            }
        }
        var relNs = NAMESPACE.as("relNs");
        var subNs = NAMESPACE.as("subNs");
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
                          .and(ASSERTION.OBJECT.eq(resolved.id));
        if (relation != null) {
            query = query.and(SUBJECT.RELATION.eq(relation.id));
        }
        return query.stream()
                    .map(r -> new Subject(new Namespace(r.value1()), r.value2(),
                                          new Relation(new Namespace(r.value3()), r.value4())));
    }

    @SuppressWarnings("unused")
    private void dump(DSLContext context, String string) {
        var pa = SUBJECT.as("parent");
        var ch = SUBJECT.as("child");
        System.out.println(string);
        System.out.println(context.select(pa.NAME.as("parent"), ch.NAME.as("child"), EDGE.TRANSITIVE)
                                  .from(pa, ch)
                                  .join(EDGE)
                                  .on(EDGE.PARENT.eq(pa.ID).and(EDGE.CHILD.eq(ch.ID)))
                                  .fetch());
        System.out.println();
        System.out.println();
    }

    private Long resolve(Namespace namespace, boolean add) throws SQLException {
        if (add) {
            add(namespace);
        }
        Record1<Long> resolved = dslCtx.select(NAMESPACE.ID)
                                       .from(NAMESPACE)
                                       .where(NAMESPACE.NAME.eq(namespace.name))
                                       .fetchOne();
        if (!add && resolved == null) {
            return null;
        }
        return resolved.value1();
    }

    private NamespacedId resolve(Object object, boolean add) throws SQLException {
        if (add) {
            add(object);
        }
        var namespace = resolve(object.namespace, add);
        if (!add && namespace == null) {
            return null;
        }
        var relation = resolve(object.relation, add);
        if (!add && relation == null) {
            return null;
        }

        var resolved = dslCtx.select(OBJECT.ID)
                             .from(OBJECT)
                             .where(OBJECT.NAMESPACE.eq(namespace)
                                                    .and(OBJECT.NAME.eq(object.name))
                                                    .and(OBJECT.RELATION.eq(relation.id)))
                             .fetchOne();
        if (!add && resolved == null) {
            return null;
        }
        return new NamespacedId(namespace, resolved.value1(), relation.id);
    }

    private NamespacedId resolve(Relation relation, boolean add) throws SQLException {
        if (add) {
            add(relation);
        }
        var namespace = resolve(relation.namespace, add);
        if (!add && namespace == null) {
            return null;
        }
        var resolved = dslCtx.select(RELATION.ID)
                             .from(RELATION)
                             .where(RELATION.NAMESPACE.eq(namespace).and(RELATION.NAME.eq(relation.name)))
                             .fetchOne();
        if (!add && resolved == null) {
            return null;
        }
        return new NamespacedId(namespace, resolved.value1(), 0L);
    }

    private NamespacedId resolve(Subject subject, boolean add) throws SQLException {
        if (add) {
            add(subject);
        }
        var namespace = resolve(subject.namespace, add);
        if (!add && namespace == null) {
            return null;
        }
        var relation = resolve(subject.relation, add);
        if (!add && relation == null) {
            return null;
        }
        var resolved = dslCtx.select(SUBJECT.ID)
                             .from(SUBJECT)
                             .where(SUBJECT.NAMESPACE.eq(namespace))
                             .and(SUBJECT.NAME.eq(subject.name))
                             .and(SUBJECT.RELATION.eq(relation.id))
                             .fetchOne();
        if (!add && resolved == null) {
            return null;
        }
        return new NamespacedId(namespace, resolved.value1(), relation.id);
    }

    /**
     * Answer the list of direct and transitive subjects, that map to the object.
     * These subjects are further filtered by the predicate Relation, if not null.
     * The query only considers assertions that match the object completely - i.e.
     * {namespace, name, relation}
     * 
     * @throws SQLException
     */
    private Stream<Subject> subjects(Relation predicate, Object object) throws SQLException {
        var resolved = resolve(object, false);
        if (resolved == null) {
            return Stream.empty();
        }

        NamespacedId relation = null;
        if (predicate != null) {
            relation = resolve(predicate, false);
            if (relation == null) {
                return Stream.empty();
            }
        }

        var subject = dslCtx.select(EDGE.PARENT.as("inferred"), EDGE.CHILD.as("direct"))
                            .from(EDGE)
                            .where(EDGE.TYPE.eq(SUBJECT_TYPE))
                            .asTable("S");

        var direct = subject.field("direct", Long.class);
        var inferred = subject.field("inferred", Long.class);

        var o = dslCtx.select(EDGE.CHILD.as("object_id"))
                      .from(EDGE)
                      .where(EDGE.TYPE.eq(OBJECT_TYPE))
                      .and(EDGE.PARENT.eq(resolved.id))
                      .union(DSL.select(DSL.val(resolved.id).as("object_id")))
                      .asTable();
        var objectId = o.field("object_id", Long.class);

        var relNs = NAMESPACE.as("relNs");
        var subNs = NAMESPACE.as("subNs");

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
        var query = relation == null ? base : base.where(SUBJECT.RELATION.eq(relation.id));
        return query.stream()
                    .map(r -> new Subject(new Namespace(r.value1()), r.value2(),
                                          new Relation(new Namespace(r.value3()), r.value4())));
    }
}
