/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import java.sql.Connection;
import java.util.concurrent.CompletableFuture;

/**
 * Oracle where write ops are JDBC stored procedure calls
 * 
 * @author hal.hildebrand
 *
 */
public class CallOracle extends AbstractOracle {

    private final Connection connection;

    public CallOracle(Connection connection) {
        super(connection);
        this.connection = connection;
    }

    @Override
    public CompletableFuture<Void> add(Assertion assertion) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.addAssertion(?, ?, ?, ?, ?, ?, ?, ?) ");
            call.setString(1, assertion.subject().namespace().name());
            call.setString(2, assertion.subject().name());
            call.setString(3, assertion.subject().relation().namespace().name());
            call.setString(4, assertion.subject().relation().name());
            call.setString(5, assertion.object().namespace().name());
            call.setString(6, assertion.object().name());
            call.setString(7, assertion.object().relation().namespace().name());
            call.setString(8, assertion.object().relation().name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> add(Namespace namespace) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.addNamespace(?) ");
            call.setString(1, namespace.name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> add(Object object) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.addObject(?, ?, ?, ?) ");
            call.setString(1, object.namespace().name());
            call.setString(2, object.name());
            call.setString(3, object.relation().namespace().name());
            call.setString(4, object.relation().name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> add(Relation relation) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.addRelation(?, ?) ");
            call.setString(1, relation.namespace().name());
            call.setString(2, relation.name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> add(Subject subject) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.addSubject(?, ?, ?, ?) ");
            call.setString(1, subject.namespace().name());
            call.setString(2, subject.name());
            call.setString(3, subject.relation().namespace().name());
            call.setString(4, subject.relation().name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> delete(Assertion assertion) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.deleteAssertion(?, ?, ?, ?, ?, ?, ?, ?) ");
            call.setString(1, assertion.subject().namespace().name());
            call.setString(2, assertion.subject().name());
            call.setString(3, assertion.subject().relation().namespace().name());
            call.setString(4, assertion.subject().relation().name());
            call.setString(5, assertion.object().namespace().name());
            call.setString(6, assertion.object().name());
            call.setString(7, assertion.object().relation().namespace().name());
            call.setString(8, assertion.object().relation().name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> delete(Namespace namespace) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.deleteNamespace(?) ");
            call.setString(1, namespace.name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> delete(Object object) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.deleteObject(?, ?, ?, ?) ");
            call.setString(1, object.namespace().name());
            call.setString(2, object.name());
            call.setString(3, object.relation().namespace().name());
            call.setString(4, object.relation().name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> delete(Relation relation) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.deleteRelation(?, ?) ");
            call.setString(1, relation.namespace().name());
            call.setString(2, relation.name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> delete(Subject subject) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.deleteSubject(?, ?, ?, ?) ");
            call.setString(1, subject.namespace().name());
            call.setString(2, subject.name());
            call.setString(3, subject.relation().namespace().name());
            call.setString(4, subject.relation().name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> map(Object parent, Object child) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.mapObject(?, ?, ?, ?, ?, ?, ?, ?) ");
            call.setString(1, parent.namespace().name());
            call.setString(2, parent.name());
            call.setString(3, parent.relation().namespace().name());
            call.setString(4, parent.relation().name());
            call.setString(5, child.namespace().name());
            call.setString(6, child.name());
            call.setString(7, child.relation().namespace().name());
            call.setString(8, child.relation().name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> map(Relation parent, Relation child) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareStatement("call delphinius.mapRelation(?, ?, ?, ?)");
            call.setString(1, parent.namespace().name());
            call.setString(2, parent.name());
            call.setString(3, child.namespace().name());
            call.setString(4, child.name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> map(Subject parent, Subject child) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareStatement(" call delphinius.mapSubject(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ");
            call.setString(1, parent.namespace().name());
            call.setString(2, parent.name());
            call.setString(3, parent.relation().namespace().name());
            call.setString(4, parent.relation().name());
            call.setString(5, child.namespace().name());
            call.setString(6, child.name());
            call.setString(7, child.relation().namespace().name());
            call.setString(8, child.relation().name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> remove(Object parent, Object child) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.removeObject(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ");
            call.setString(1, parent.namespace().name());
            call.setString(2, parent.name());
            call.setString(3, parent.relation().namespace().name());
            call.setString(4, parent.relation().name());
            call.setString(5, child.namespace().name());
            call.setString(6, child.name());
            call.setString(7, child.relation().namespace().name());
            call.setString(8, child.relation().name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> remove(Relation parent, Relation child) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.removeRelation(?, ?, ?, ?) ");
            call.setString(1, parent.namespace().name());
            call.setString(2, parent.name());
            call.setString(3, child.namespace().name());
            call.setString(4, child.name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Void> remove(Subject parent, Subject child) {
        var fs = new CompletableFuture<Void>();
        try {
            var call = connection.prepareCall("call delphinius.removeSubject(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ");
            call.setString(1, parent.namespace().name());
            call.setString(2, parent.name());
            call.setString(3, parent.relation().namespace().name());
            call.setString(4, parent.relation().name());
            call.setString(5, child.namespace().name());
            call.setString(6, child.name());
            call.setString(7, child.relation().namespace().name());
            call.setString(8, child.relation().name());

            call.execute();
            connection.commit();
            fs.complete(null);
        } catch (Exception e) {
            fs.completeExceptionally(e);
        }
        return fs;
    }

}
