/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import org.h2.result.Row;
import org.h2.table.Table;

public class CdcEvent {
    public final short operation;
    public final Row   row;
    public final Table table;

    public CdcEvent(Table table, short operation, Row row) {
        this.table = table;
        this.operation = operation;
        this.row = row;
    }
}