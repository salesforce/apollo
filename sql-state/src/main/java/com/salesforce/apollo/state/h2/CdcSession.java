/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.h2;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.result.Row;
import org.h2.table.Table;

/**
 * @author hal.hildebrand
 *
 */
public class CdcSession extends Session {

    public static class CdcEvent {
        public final short operation;
        public final Row   row;
        public final Table table;

        public CdcEvent(Table table, short operation, Row row) {
            this.table = table;
            this.operation = operation;
            this.row = row;
        }
    }

    private Cdc cdc = new NullCapture();

    public CdcSession(Database database, User user, int id) {
        super(database, user, id);
    }

    @Override
    public void cancel() {
        super.cancel();
    }

    @Override
    public void log(Table table, short operation, Row row) {
        cdc.log(table, operation, row);
        super.log(table, operation, row);
    }

    public void setCdc(Cdc cdc) {
        this.cdc = cdc;
    }

    @Override
    public void begin() {
        super.begin();
    }
}
