/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.liquibase;

import org.h2.util.DateTimeUtils;

import liquibase.Scope;
import liquibase.change.ColumnConfig;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.StandardChangeLogHistoryService;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.statement.core.SelectFromDatabaseChangeLogStatement;
import liquibase.statement.core.TagDatabaseStatement;

/**
 * @author hal.hildebrand
 *
 */
public class ReplicatedChangeLogHistoryService extends StandardChangeLogHistoryService {

    private String deploymentId;

    @Override
    public String getDeploymentId() {
        return this.deploymentId;
    }

    @Override
    public void resetDeploymentId() {
        this.deploymentId = null;
    }

    @Override
    public void generateDeploymentId() {
        if (this.deploymentId == null) {
            final var now = DateTimeUtils.now();
            String dateString = String.format("%s.%s", now.getEpochSecond(), now.getNano());
            this.deploymentId = dateString.substring(dateString.length() - 10);
        }
    }
    @Override
    public void tag(final String tagString) throws DatabaseException {
        Database database = getDatabase();
        Executor executor = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database);
        int totalRows = Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", database).queryForInt(new
            SelectFromDatabaseChangeLogStatement(new ColumnConfig().setName("COUNT(*)", true)));
        if (totalRows == 0) {
            final var now = DateTimeUtils.now();
            String dateString = String.format("%s.%s", now.getEpochSecond(), now.getNano());
            ChangeSet emptyChangeSet = new ChangeSet(dateString, "liquibase",
                false,false, "liquibase-internal", null, null,
                getDatabase().getObjectQuotingStrategy(), null);
            this.setExecType(emptyChangeSet, ChangeSet.ExecType.EXECUTED);
        }

        executor.execute(new TagDatabaseStatement(tagString));
        getDatabase().commit();

        final var ranChangeSets = this.getRanChangeSets();
        if (ranChangeSets != null && !ranChangeSets.isEmpty()) {
            ranChangeSets.get(ranChangeSets.size() - 1).setTag(tagString);
        }
    }
}
