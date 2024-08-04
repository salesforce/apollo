/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.delphinius;

import com.salesforce.apollo.choam.support.InvalidTransaction;
import com.salesforce.apollo.delphinius.AbstractOracle;
import com.salesforce.apollo.state.Mutator;
import org.joou.ULong;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.salesforce.apollo.choam.Session.retryNesting;

/**
 * Oracle where write ops are JDBC stored procedure calls operating on the shared sql state
 *
 * @author hal.hildebrand
 */
public class ShardedOracle extends AbstractOracle {

    private final Mutator         mutator;
    private final Duration        timeout;
    private final Supplier<ULong> clock;

    public ShardedOracle(Connection connection, Mutator mutator, Duration timeout, Supplier<ULong> clock) {
        super(connection);
        this.mutator = mutator;
        this.timeout = timeout;
        this.clock = clock;
    }

    @Override
    public CompletableFuture<Asserted> add(Assertion assertion) {
        var call = mutator.call("? = call delphinius.addAssertion(?, ?, ?, ?, ?, ?, ?, ?) ",
                                Collections.singletonList(JDBCType.BOOLEAN), assertion.subject().namespace().name(),
                                assertion.subject().name(), assertion.subject().relation().namespace().name(),
                                assertion.subject().relation().name(), assertion.object().namespace().name(),
                                assertion.object().name(), assertion.object().relation().namespace().name(),
                                assertion.object().relation().name());
        try {
            return mutator.execute(call, timeout)
                          .thenApply(r -> new Asserted(clock.get(), (Boolean) r.outValues.getFirst()));
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<Asserted>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> add(Namespace namespace) {
        var call = mutator.call("call delphinius.addNamespace(?) ", namespace.name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> add(Object object) {
        var call = mutator.call("call delphinius.addObject(?, ?, ?, ?) ", object.namespace().name(), object.name(),
                                object.relation().namespace().name(), object.relation().name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> add(Relation relation) {
        var call = mutator.call("call delphinius.addRelation(?, ?) ", relation.namespace().name(), relation.name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> add(Subject subject) {
        var call = mutator.call("call delphinius.addSubject(?, ?, ?, ?) ", subject.namespace().name(), subject.name(),
                                subject.relation().namespace().name(), subject.relation().name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    public CompletableFuture<Asserted> add(Assertion assertion, int retries) {
        return retryNesting(() -> add(assertion), retries);
    }

    public CompletableFuture<ULong> add(Namespace namespace, int retries) {
        return retryNesting(() -> add(namespace), retries);
    }

    public CompletableFuture<ULong> add(Object object, int retries) {
        return retryNesting(() -> add(object), retries);
    }

    public CompletableFuture<ULong> add(Relation relation, int retries) {
        return retryNesting(() -> add(relation), retries);
    }

    public CompletableFuture<ULong> add(Subject subject, int retries) {
        return retryNesting(() -> add(subject), retries);
    }

    @Override
    public boolean check(Assertion assertion, ULong valid) throws SQLException {
        if (valid.compareTo(clock.get()) > 0) {
            return false;
        }
        return check(assertion);
    }

    @Override
    public CompletableFuture<ULong> delete(Assertion assertion) {
        var call = mutator.call("call delphinius.deleteAssertion(?, ?, ?, ?, ?, ?, ?, ?) ",
                                assertion.subject().namespace().name(), assertion.subject().name(),
                                assertion.subject().relation().namespace().name(),
                                assertion.subject().relation().name(), assertion.object().namespace().name(),
                                assertion.object().name(), assertion.object().relation().namespace().name(),
                                assertion.object().relation().name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> delete(Namespace namespace) {
        var call = mutator.call("call delphinius.deleteNamespace(?) ", namespace.name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> delete(Object object) {
        var call = mutator.call("call delphinius.deleteObject(?, ?, ?, ?) ", object.namespace().name(), object.name(),
                                object.relation().namespace().name(), object.relation().name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> delete(Relation relation) {
        var call = mutator.call("call delphinius.deleteRelation(?, ?) ", relation.namespace().name(), relation.name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> delete(Subject subject) {
        var call = mutator.call("call delphinius.deleteSubject(?, ?, ?, ?) ", subject.namespace().name(),
                                subject.name(), subject.relation().namespace().name(), subject.relation().name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    public CompletableFuture<ULong> delete(Assertion assertion, int retries) {
        return retryNesting(() -> delete(assertion), retries);
    }

    public CompletableFuture<ULong> delete(Namespace namespace, int retries) {
        return retryNesting(() -> delete(namespace), retries);
    }

    public CompletableFuture<ULong> delete(Object object, int retries) {
        return retryNesting(() -> delete(object), retries);
    }

    public CompletableFuture<ULong> delete(Relation relation, int retries) {
        return retryNesting(() -> delete(relation), retries);
    }

    public CompletableFuture<ULong> delete(Subject subject, int retries) {
        return retryNesting(() -> delete(subject), retries);
    }

    @Override
    public CompletableFuture<ULong> map(Object parent, Object child) {
        var call = mutator.call("call delphinius.mapObject(?, ?, ?, ?, ?, ?, ?, ?) ", parent.namespace().name(),
                                parent.name(), parent.relation().namespace().name(), parent.relation().name(),
                                child.namespace().name(), child.name(), child.relation().namespace().name(),
                                child.relation().name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> map(Relation parent, Relation child) {
        var call = mutator.call("call delphinius.mapRelation(?, ?, ?, ?)", parent.namespace().name(), parent.name(),
                                child.namespace().name(), child.name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> map(Subject parent, Subject child) {
        var call = mutator.call("call delphinius.mapSubject(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ",
                                parent.namespace().name(), parent.name(), parent.relation().namespace().name(),
                                parent.relation().name(), child.namespace().name(), child.name(),
                                child.relation().namespace().name(), child.relation().name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    public CompletableFuture<ULong> map(Object parent, Object child, int retries) {
        return retryNesting(() -> map(parent, child), retries);
    }

    public CompletableFuture<ULong> map(Relation parent, Relation child, int retries) {
        return retryNesting(() -> map(parent, child), retries);
    }

    public CompletableFuture<ULong> map(Subject parent, Subject child, int retries) {
        return retryNesting(() -> map(parent, child), retries);
    }

    @Override
    public CompletableFuture<ULong> remove(Object parent, Object child) {
        var call = mutator.call("call delphinius.removeObject(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ",
                                parent.namespace().name(), parent.name(), parent.relation().namespace().name(),
                                parent.relation().name(), child.namespace().name(), child.name(),
                                child.relation().namespace().name(), child.relation().name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> remove(Relation parent, Relation child) {
        var call = mutator.call("call delphinius.removeRelation(?, ?, ?, ?) ", parent.namespace().name(), parent.name(),
                                child.namespace().name(), child.name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<ULong> remove(Subject parent, Subject child) {
        var call = mutator.call("call delphinius.removeSubject(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ",
                                parent.namespace().name(), parent.name(), parent.relation().namespace().name(),
                                parent.relation().name(), child.namespace().name(), child.name(),
                                child.relation().namespace().name(), child.relation().name());
        try {
            return mutator.execute(call, timeout).thenApply(r -> clock.get());
        } catch (InvalidTransaction e) {
            var f = new CompletableFuture<ULong>();
            f.completeExceptionally(e);
            return f;
        }
    }

    public CompletableFuture<ULong> remove(Object parent, Object child, int retries) {
        return retryNesting(() -> remove(parent, child), retries);
    }

    public CompletableFuture<ULong> remove(Relation parent, Relation child, int retries) {
        return retryNesting(() -> remove(parent, child), retries);
    }

    public CompletableFuture<ULong> remove(Subject parent, Subject child, int retries) {
        return retryNesting(() -> remove(parent, child), retries);
    }

}
