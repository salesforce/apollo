/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import java.sql.Connection;
import java.sql.SQLException;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

/**
 * @author hal.hildebrand
 *
 */
public class DirectOracle extends AbstractOracle {

    private final DSLContext dslCtx;

    public DirectOracle(Connection connection) {
        this(DSL.using(connection, SQLDialect.H2));
    }

    public DirectOracle(DSLContext dslCtx) {
        super(dslCtx);
        this.dslCtx = dslCtx;
    }

    /**
     * Add an Assertion. The subject and object of the assertion will also be added
     * if they do not exist
     */
    public void add(Assertion assertion) throws SQLException {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), assertion);
        });
    }

    /**
     * Add a Namespace.
     */
    public void add(Namespace namespace) throws SQLException {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), namespace);
        });
    }

    /**
     * Add an Object.
     */
    public void add(Object object) throws SQLException {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), object);
        });
    }

    /**
     * Add a Relation
     */
    public void add(Relation relation) throws SQLException {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), relation);
        });
    }

    /**
     * Add a Subject
     */
    public void add(Subject subject) throws SQLException {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), subject);
        });
    }

    /**
     * Delete an assertion. Only the assertion is deleted, not the subject nor
     * object of the assertion.
     */
    public void delete(Assertion assertion) throws SQLException {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), assertion);
        });
    }

    /**
     * Delete a Namespace. All objects, subjects, relations and assertions that
     * reference this namespace will also be deleted.
     */
    @Override
    public void delete(Namespace namespace) throws SQLException {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), namespace);
        });
    }

    /**
     * Delete an Object. All dependant uses of the object (mappings, Assertions) are
     * removed as well.
     */
    public void delete(Object object) throws SQLException {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), object);
        });
    }

    /**
     * Delete an Relation. All dependant uses of the relation (mappings, Subject,
     * Object and Assertions) are removed as well.
     */
    public void delete(Relation relation) throws SQLException {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), relation);
        });
    }

    /**
     * Delete an Subject. All dependant uses of the subject (mappings and
     * Assertions) are removed as well.
     */
    public void delete(Subject subject) throws SQLException {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), subject);
        });
    }

    /**
     * Map the parent object to the child
     */
    public void map(Object parent, Object child) throws SQLException {
        dslCtx.transaction(ctx -> {
            map(parent, DSL.using(ctx), child);
        });
    }

    /**
     * Map the parent relation to the child
     */
    public void map(Relation parent, Relation child) throws SQLException {
        dslCtx.transaction(ctx -> {
            map(parent, DSL.using(ctx), child);
        });
    }

    /**
     * Map the parent subject to the child
     */
    public void map(Subject parent, Subject child) throws SQLException {
        dslCtx.transaction(ctx -> {
            map(parent, DSL.using(ctx), child);
        });
    }

    /**
     * Remove the mapping between the parent and the child objects
     */
    public void remove(Object parent, Object child) throws SQLException {
        dslCtx.transaction(ctx -> {
            remove(parent, DSL.using(ctx), child);
        });
    }

    /**
     * Remove the mapping between the parent and the child relations
     */
    public void remove(Relation parent, Relation child) throws SQLException {
        dslCtx.transaction(ctx -> {
            remove(parent, DSL.using(ctx), child);
        });
    }

    /**
     * Remove the mapping between the parent and the child subects
     */
    public void remove(Subject parent, Subject child) throws SQLException {
        dslCtx.transaction(ctx -> {
            remove(parent, DSL.using(ctx), child);
        });
    }
}
