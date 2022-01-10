/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * @author hal.hildebrand
 *
 */
public interface Oracle {

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

    record NamespacedId(Long namespace, Long id, Long relation) {}

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

    /** Grounding for all the domains */
    Namespace NO_NAMESPACE = new Namespace("");
    Relation  NO_RELATION  = new Relation(NO_NAMESPACE, "");
    Subject   NO_SUBJECT   = new Subject(NO_NAMESPACE, "", NO_RELATION);
    Object    NO_OBJECT    = new Object(NO_NAMESPACE, "", NO_RELATION);
    Assertion NO_ASSERTION = new Assertion(NO_SUBJECT, NO_OBJECT);

    // Types for DAG
    String OBJECT_TYPE   = "o";
    String RELATION_TYPE = "r";
    String SUBJECT_TYPE  = "s";

    static Namespace namespace(String name) {
        return new Namespace(name);
    }

    /**
     * Add an Assertion. The subject and object of the assertion will also be added
     * if they do not exist
     */
    CompletableFuture<Void> add(Assertion assertion);

    /**
     * Add a Namespace.
     */
    CompletableFuture<Void> add(Namespace namespace);

    /**
     * Add an Object.
     */
    CompletableFuture<Void> add(Object object);

    /**
     * Add a Relation
     */
    CompletableFuture<Void> add(Relation relation);

    /**
     * Add a Subject
     */
    CompletableFuture<Void> add(Subject subject);

    /**
     * Check the assertion.
     * 
     * @return true if the assertion is made, false if not
     */
    boolean check(Assertion assertion) throws SQLException;

    /**
     * Delete an assertion. Only the assertion is deleted, not the subject nor
     * object of the assertion.
     */
    CompletableFuture<Void> delete(Assertion assertion);

    /**
     * Delete an Object. All dependant uses of the object (mappings, Assertions) are
     * removed as well.
     */
    CompletableFuture<Void> delete(Object object);

    /**
     * Delete an Relation. All dependant uses of the relation (mappings, Subject,
     * Object and Assertions) are removed as well.
     */
    CompletableFuture<Void> delete(Relation relation);

    /**
     * Delete an Subject. All dependant uses of the subject (mappings and
     * Assertions) are removed as well.
     */
    CompletableFuture<Void> delete(Subject subject);

    /**
     * Answer the list of Subjects, both direct and transitive Subjects, that map to
     * the supplied object. The query only considers subjects with assertions that
     * match the object completely - i.e. {namespace, name, relation}
     * 
     * @throws SQLException
     */
    List<Subject> expand(Object object) throws SQLException;

    /**
     * Answer the list of Subjects, both direct and transitive, that map to the
     * object from subjects that have the supplied predicate as their relation. The
     * query only considers assertions that match the object completely - i.e.
     * {namespace, name, relation}
     * 
     * @throws SQLException
     */
    List<Subject> expand(Relation predicate, Object object) throws SQLException;

    /**
     * Answer the list of direct and transitive Objects that map to the subject from
     * objects that have the supplied predicate as their relation. The query only
     * considers assertions that match the subject completely - i.e. {namespace,
     * name, relation}
     * 
     * @throws SQLException
     */
    List<Object> expand(Relation predicate, Subject subject) throws SQLException;

    /**
     * Answer the list of direct and transitive Objects that map to the supplied
     * subject. The query only considers objects with assertions that match the
     * subject completely - i.e. {namespace, name, relation}
     * 
     * @throws SQLException
     */
    List<Object> expand(Subject subject) throws SQLException;

    /**
     * Map the parent object to the child
     */
    CompletableFuture<Void> map(Object parent, Object child);

    /**
     * Map the parent relation to the child
     */
    CompletableFuture<Void> map(Relation parent, Relation child);

    /**
     * Map the parent subject to the child
     */
    CompletableFuture<Void> map(Subject parent, Subject child);

    /**
     * Answer the list of direct Subjects that map to the supplied objects. The
     * query only considers subjects with assertions that match the objects
     * completely - i.e. {namespace, name, relation}
     * 
     * @throws SQLException
     */
    List<Subject> read(Object... objects) throws SQLException;

    /**
     * Answer the list of direct Subjects that map to the supplied objects. The
     * query only considers subjects with assertions that match the objects
     * completely - i.e. {namespace, name, relation} and only the subjects that have
     * the matching predicate
     * 
     * @throws SQLException
     */
    List<Subject> read(Relation predicate, Object... objects) throws SQLException;

    /**
     * Answer the list of direct Objects that map to the supplied subjects. The
     * query only considers objects with assertions that match the subjects
     * completely - i.e. {namespace, name, relation} and only the objects that have
     * the matching predicate
     * 
     * @throws SQLException
     */
    List<Object> read(Relation predicate, Subject... subjects) throws SQLException;

    /**
     * Answer the list of direct Objects that map to the supplied subjects. The
     * query only considers objects with assertions that match the subjects
     * completely - i.e. {namespace, name, relation}
     * 
     * @throws SQLException
     */
    List<Object> read(Subject... subjects) throws SQLException;

    /**
     * Remove the mapping between the parent and the child objects
     */
    CompletableFuture<Void> remove(Object parent, Object child);

    /**
     * Remove the mapping between the parent and the child relations
     */
    CompletableFuture<Void> remove(Relation parent, Relation child);

    /**
     * Remove the mapping between the parent and the child subects
     */
    CompletableFuture<Void> remove(Subject parent, Subject child);

    /**
     * Answer the list of direct and transitive subjects that map to the object.
     * These subjects may be further filtered by the predicate Relation, if not
     * null. The query only considers assertions that match the object completely -
     * i.e. {namespace, name, relation}
     * 
     * @throws SQLException
     */
    Stream<Subject> subjects(Relation predicate, Object object) throws SQLException;

    /**
     * Delete a Namespace. All objects, subjects, relations and assertions that
     * reference this namespace will also be deleted.
     */
    CompletableFuture<Void> delete(Namespace namespace);

}
