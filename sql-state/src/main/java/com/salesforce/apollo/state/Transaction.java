/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.h2.result.Row;
import org.h2.table.Table;

import com.salesforce.apollo.state.h2.Cdc;

/**
 * @author hal.hildebrand
 *
 */
public class Transaction implements Cdc {

    public static class Statement {
        public final List<SqlIdentifier> arguments = new ArrayList<>();
        public final SqlNode             query;

        public Statement(SqlNode query) {
            this.query = query;
        }
    }

    private final List<List<CdcEvent>> changes    = new ArrayList<>();
    private final List<Statement>      statements = new ArrayList<>();

    public List<CdcEvent> getChanges() {
        return changes.stream().flatMap(events -> events.stream()).collect(Collectors.toList());
    }

    public List<Statement> getStatements() {
        return statements;
    }

    @Override
    public void log(Table table, short operation, Row row) {
        changes.get(changes.size() - 1).add(new CdcEvent(table, operation, row));
    }

    public void process(SqlNode statement) {
        statements.add(parse(statement));
        changes.add(new ArrayList<>());
    }

    private Statement parse(SqlNode statement) {
        return new Statement(statement);
    }

}
