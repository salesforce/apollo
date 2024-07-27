/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import org.joou.ULong;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * @author hal.hildebrand
 */
public interface Oracle {

    // Types for DAG
    String OBJECT_TYPE   = "o";
    String RELATION_TYPE = "r";
    String SUBJECT_TYPE  = "s";

    static Namespace namespace(String name) {
        return new Namespace(name);
    }

    /**
     * Add an Assertion. The subject and object of the assertion will also be added if they do not exist
     *
     * @return the future returning the time value when this assertion is committed
     */
    CompletableFuture<ULong> add(Assertion assertion);

    /**
     * Add a Namespace.
     *
     * @return the future returning the time value when this namespace is committed
     */
    CompletableFuture<ULong> add(Namespace namespace);

    /**
     * Add an Object.
     *
     * @return the future returning the time value when this object is committed
     */
    CompletableFuture<ULong> add(Object object);

    /**
     * Add a Relation
     *
     * @return the future returning the time value when this relation is committed
     */
    CompletableFuture<ULong> add(Relation relation);

    /**
     * Add a Subject
     *
     * @return the future returning the time value when this subject is committed
     */
    CompletableFuture<ULong> add(Subject subject);

    /**
     * Check the assertion.
     *
     * @return true if the assertion is made, false if not
     */
    boolean check(Assertion assertion) throws SQLException;

    /**
     * Delete an assertion. Only the assertion is deleted, not the subject nor object of the assertion.
     *
     * @return the future returning the time value when this assertion delete is committed
     */
    CompletableFuture<ULong> delete(Assertion assertion);

    /**
     * Delete a Namespace. All objects, subjects, relations and assertions that reference this namespace will also be
     * deleted.
     *
     * @return the future returning the time value when this namespace delete is committed
     */
    CompletableFuture<ULong> delete(Namespace namespace);

    /**
     * Delete an Object. All dependent uses of the object (mappings, Assertions) are removed as well.
     *
     * @return the future returning the time value when this object delete is committed
     */
    CompletableFuture<ULong> delete(Object object);

    /**
     * Delete a Relation. All dependent uses of the relation (mappings, Subject, Object and Assertions) are removed as
     * well.
     *
     * @return the future returning the time value when this relation delete is committed
     */
    CompletableFuture<ULong> delete(Relation relation);

    /**
     * Delete an Subject. All dependent uses of the subject (mappings and Assertions) are removed as well.
     *
     * @return the future returning the time value when this subject delete is committed
     */
    CompletableFuture<ULong> delete(Subject subject);

    /**
     * Answer the list of Subjects, both direct and transitive Subjects, that map to the supplied object. The query only
     * considers subjects with assertions that match the object completely - i.e. {namespace, name, relation}
     *
     * @throws SQLException
     */
    List<Subject> expand(Object object) throws SQLException;

    /**
     * Answer the list of Subjects, both direct and transitive, that map to the object from subjects that have the
     * supplied predicate as their relation. The query only considers assertions that match the object completely - i.e.
     * {namespace, name, relation}
     *
     * @throws SQLException
     */
    List<Subject> expand(Relation predicate, Object object) throws SQLException;

    /**
     * Answer the list of direct and transitive Objects that map to the subject from objects that have the supplied
     * predicate as their relation. The query only considers assertions that match the subject completely - i.e.
     * {namespace, name, relation}
     *
     * @throws SQLException
     */
    List<Object> expand(Relation predicate, Subject subject) throws SQLException;

    /**
     * Answer the list of direct and transitive Objects that map to the supplied subject. The query only considers
     * objects with assertions that match the subject completely - i.e. {namespace, name, relation}
     *
     * @throws SQLException
     */
    List<Object> expand(Subject subject) throws SQLException;

    /**
     * Map the parent object to the child
     *
     * @return the future returning the time value when this object mapping is committed
     */
    CompletableFuture<ULong> map(Object parent, Object child);

    /**
     * Map the parent relation to the child
     *
     * @return the future returning the time value when this relation mapping is committed
     */
    CompletableFuture<ULong> map(Relation parent, Relation child);

    /**
     * Map the parent subject to the child
     *
     * @return the future returning the time value when this subject mapping is committed
     */
    CompletableFuture<ULong> map(Subject parent, Subject child);

    /**
     * Answer the list of direct Subjects that map to the supplied objects. The query only considers subjects with
     * assertions that match the objects completely - i.e. {namespace, name, relation}
     *
     * @throws SQLException
     */
    List<Subject> read(Object... objects) throws SQLException;

    /**
     * Answer the list of direct Subjects that map to the supplied objects. The query only considers subjects with
     * assertions that match the objects completely - i.e. {namespace, name, relation} and only the subjects that have
     * the matching predicate
     *
     * @throws SQLException
     */
    List<Subject> read(Relation predicate, Object... objects) throws SQLException;

    /**
     * Answer the list of direct Objects that map to the supplied subjects. The query only considers objects with
     * assertions that match the subjects completely - i.e. {namespace, name, relation} and only the objects that have
     * the matching predicate
     *
     * @throws SQLException
     */
    List<Object> read(Relation predicate, Subject... subjects) throws SQLException;

    /**
     * Answer the list of direct Objects that map to the supplied subjects. The query only considers objects with
     * assertions that match the subjects completely - i.e. {namespace, name, relation}
     *
     * @throws SQLException
     */
    List<Object> read(Subject... subjects) throws SQLException;

    /**
     * Remove the mapping between the parent and the child objects
     *
     * @return the future returning the time value when this object mapping removal is committed
     */
    CompletableFuture<ULong> remove(Object parent, Object child);

    /**
     * Remove the mapping between the parent and the child relations
     *
     * @return the future returning the time value when this relation mapping removal is committed
     */
    CompletableFuture<ULong> remove(Relation parent, Relation child);

    /**
     * Remove the mapping between the parent and the child subjects
     *
     * @return the future returning the time value when this subject mapping removal is committed
     */
    CompletableFuture<ULong> remove(Subject parent, Subject child);

    /**
     * Answer the list of direct and transitive subjects that map to the object. These subjects may be further filtered
     * by the predicate Relation, if not null. The query only considers assertions that match the object completely -
     * i.e. {namespace, name, relation}
     *
     * @throws SQLException
     */
    Stream<Subject> subjects(Relation predicate, Object object) throws SQLException;

    /** A Namespace **/
    record Namespace(String name) {

        /** Grounding for all the domains */
        public static final Namespace NO_NAMESPACE = new Namespace("");

        public Object object(String name, Relation relation) {
            return new Object(this, name, relation);
        }

        public Relation relation(String name) {
            return new Relation(this, name);
        }

        public Subject subject(String name) {
            return new Subject(this, name, Relation.NO_RELATION);
        }

        public Subject subject(String name, Relation relation) {
            return new Subject(this, name, relation);
        }
    }

    record NamespacedId(Long namespace, Long id, Long relation) {
    }

    /** A Subject **/
    record Subject(Namespace namespace, String name, Relation relation) {

        public static final Subject NO_SUBJECT = new Subject(Namespace.NO_NAMESPACE, "", Relation.NO_RELATION);

        public Assertion assertion(Object object) {
            return new Assertion(this, object);
        }

        @Override
        public String toString() {
            return namespace.name + ":" + name + (relation.equals(Relation.NO_RELATION) ? "" : "#" + relation);
        }
    }

    /** An Object **/
    record Object(Namespace namespace, String name, Relation relation) {

        public static final Object NO_OBJECT = new Object(Namespace.NO_NAMESPACE, "", Relation.NO_RELATION);

        public Assertion assertion(Subject subject) {
            return new Assertion(subject, this);
        }

        @Override
        public String toString() {
            return namespace.name + ":" + name + (relation.equals(Relation.NO_RELATION) ? "" : "#" + relation);
        }
    }

    /** A Relation **/
    record Relation(Namespace namespace, String name) {
        public static final Relation NO_RELATION = new Relation(Namespace.NO_NAMESPACE, "");

        @Override
        public String toString() {
            return namespace.name + ":" + name;
        }
    }

    /** An Assertion **/
    record Assertion(Subject subject, Object object) {
        public static final Assertion NO_ASSERTION = new Assertion(Subject.NO_SUBJECT, Object.NO_OBJECT);

        @Override
        public String toString() {
            return subject + "@" + object;
        }
    }

}
