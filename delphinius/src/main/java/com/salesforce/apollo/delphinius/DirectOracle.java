/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.joou.ULong;

import java.sql.Connection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @author hal.hildebrand
 */
public class DirectOracle extends AbstractOracle {

    private final DSLContext      dslCtx;
    private final Supplier<ULong> clock;

    public DirectOracle(Connection connection) {
        this(connection, () -> ULong.valueOf(System.currentTimeMillis()));
    }

    public DirectOracle(Connection connection, Supplier<ULong> clock) {
        this(DSL.using(connection, SQLDialect.H2), clock);
    }

    public DirectOracle(DSLContext dslCtx, Supplier<ULong> clock) {
        super(dslCtx);
        this.dslCtx = dslCtx;
        this.clock = clock;
    }

    /**
     * Add an Assertion. The subject and object of the assertion will also be added if they do not exist
     */
    public CompletableFuture<ULong> add(Assertion assertion) {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), assertion);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Add a Namespace.
     */
    public CompletableFuture<ULong> add(Namespace namespace) {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), namespace);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Add an Object.
     */
    public CompletableFuture<ULong> add(Object object) {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), object);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Add a Relation
     */
    public CompletableFuture<ULong> add(Relation relation) {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), relation);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Add a Subject
     */
    public CompletableFuture<ULong> add(Subject subject) {
        dslCtx.transaction(ctx -> {
            add(DSL.using(ctx), subject);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Delete an assertion. Only the assertion is deleted, not the subject nor object of the assertion.
     */
    public CompletableFuture<ULong> delete(Assertion assertion) {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), assertion);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Delete a Namespace. All objects, subjects, relations and assertions that reference this namespace will also be
     * deleted.
     */
    @Override
    public CompletableFuture<ULong> delete(Namespace namespace) {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), namespace);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Delete an Object. All dependent uses of the object (mappings, Assertions) are removed as well.
     */
    public CompletableFuture<ULong> delete(Object object) {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), object);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Delete an Relation. All dependent uses of the relation (mappings, Subject, Object and Assertions) are removed as
     * well.
     */
    public CompletableFuture<ULong> delete(Relation relation) {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), relation);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Delete an Subject. All dependant uses of the subject (mappings and Assertions) are removed as well.
     */
    public CompletableFuture<ULong> delete(Subject subject) {
        dslCtx.transaction(ctx -> {
            delete(DSL.using(ctx), subject);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Map the parent object to the child
     */
    public CompletableFuture<ULong> map(Object parent, Object child) {
        dslCtx.transaction(ctx -> {
            map(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Map the parent relation to the child
     */
    public CompletableFuture<ULong> map(Relation parent, Relation child) {
        dslCtx.transaction(ctx -> {
            map(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Map the parent subject to the child
     */
    public CompletableFuture<ULong> map(Subject parent, Subject child) {
        dslCtx.transaction(ctx -> {
            map(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Remove the mapping between the parent and the child objects
     */
    public CompletableFuture<ULong> remove(Object parent, Object child) {
        dslCtx.transaction(ctx -> {
            remove(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Remove the mapping between the parent and the child relations
     */
    public CompletableFuture<ULong> remove(Relation parent, Relation child) {
        dslCtx.transaction(ctx -> {
            remove(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }

    /**
     * Remove the mapping between the parent and the child subects
     */
    public CompletableFuture<ULong> remove(Subject parent, Subject child) {
        dslCtx.transaction(ctx -> {
            remove(parent, DSL.using(ctx), child);
        });
        var fs = new CompletableFuture<ULong>();
        fs.complete(clock.get());
        return fs;
    }
}
