/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.ScopeManager;
import liquibase.database.core.MockDatabase;
import liquibase.exception.LiquibaseException;

/**
 * @author hal.hildebrand
 *
 */
public class ThreadLocalScopeManager extends ScopeManager {

    private ThreadLocalScopeManager() {
    }

    private static final ThreadLocal<Scope> CURRENT_SCOPE = new ThreadLocal<>() {
        @Override
        protected Scope initialValue() {
            return defaultScope;
        }
    };

    private static Scope defaultScope;

    public static void initialize() {
        try {
            new Liquibase((String) null, new NullResourceAccessor(), new MockDatabase()).close();
        } catch (LiquibaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        defaultScope = Scope.getCurrentScope();
        Scope.setScopeManager(new ThreadLocalScopeManager());
    }

    @Override
    public Scope getCurrentScope() {
        return CURRENT_SCOPE.get();
    }

    @Override
    protected Scope init(Scope scope) throws Exception {
        return scope;
    }

    @Override
    protected void setCurrentScope(Scope scope) {
        CURRENT_SCOPE.set(scope);
    }
}
