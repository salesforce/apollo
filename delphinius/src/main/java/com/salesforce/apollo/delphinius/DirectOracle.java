/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import java.sql.Connection;
import java.util.concurrent.CompletableFuture;

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
    public CompletableFuture<Void> add(Assertion assertion) {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), assertion);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Add a Namespace.
     */
    public CompletableFuture<Void> add(Namespace namespace) {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), namespace);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Add an Object.
     */
    public CompletableFuture<Void> add(Object object) {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), object);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Add a Relation
     */
    public CompletableFuture<Void> add(Relation relation) {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), relation);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Add a Subject
     */
    public CompletableFuture<Void> add(Subject subject) {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), subject);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Delete an assertion. Only the assertion is deleted, not the subject nor
     * object of the assertion.
     */
    public CompletableFuture<Void> delete(Assertion assertion) {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), assertion);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Delete a Namespace. All objects, subjects, relations and assertions that
     * reference this namespace will also be deleted.
     */
    @Override
    public CompletableFuture<Void> delete(Namespace namespace) {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), namespace);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Delete an Object. All dependant uses of the object (mappings, Assertions) are
     * removed as well.
     */
    public CompletableFuture<Void> delete(Object object) {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), object);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Delete an Relation. All dependant uses of the relation (mappings, Subject,
     * Object and Assertions) are removed as well.
     */
    public CompletableFuture<Void> delete(Relation relation) {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), relation);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Delete an Subject. All dependant uses of the subject (mappings and
     * Assertions) are removed as well.
     */
    public CompletableFuture<Void> delete(Subject subject) {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), subject);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Map the parent object to the child
     */
    public CompletableFuture<Void> map(Object parent, Object child) {
        dslCtx.transaction(ctx -> {
            map(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Map the parent relation to the child
     */
    public CompletableFuture<Void> map(Relation parent, Relation child) {
        dslCtx.transaction(ctx -> {
            map(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Map the parent subject to the child
     */
    public CompletableFuture<Void> map(Subject parent, Subject child) {
        dslCtx.transaction(ctx -> {
            map(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Remove the mapping between the parent and the child objects
     */
    public CompletableFuture<Void> remove(Object parent, Object child) {
        dslCtx.transaction(ctx -> {
            remove(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Remove the mapping between the parent and the child relations
     */
    public CompletableFuture<Void> remove(Relation parent, Relation child) {
        dslCtx.transaction(ctx -> {
            remove(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }

    /**
     * Remove the mapping between the parent and the child subects
     */
    public CompletableFuture<Void> remove(Subject parent, Subject child) {
        dslCtx.transaction(ctx -> {
            remove(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<Void>();
        fs.complete(null);
        return fs;
    }
}
