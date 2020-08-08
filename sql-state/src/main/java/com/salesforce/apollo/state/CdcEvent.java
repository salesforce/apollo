/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import org.h2.engine.Constants;
import org.h2.result.Row;
import org.h2.store.Data;
import org.h2.table.Table;
import org.h2.value.Value;

public class CdcEvent {
    public final short operation;
    public final Row   row;
    public final Table table;

    public CdcEvent(Table table, short operation, Row row) {
        this.table = table;
        this.operation = operation;
        this.row = row;
    }

    public void append(Data buff) {
        int p = buff.length();
        buff.writeInt(0);
        buff.writeInt(operation);
        buff.writeByte(row.isDeleted() ? (byte) 1 : (byte) 0);
        buff.writeInt(table.getId());
        buff.writeLong(row.getKey());
        int count = row.getColumnCount();
        buff.writeInt(count);
        for (int i = 0; i < count; i++) {
            Value v = row.getValue(i);
            buff.checkCapacity(buff.getValueLen(v));
            buff.writeValue(v);
        }
        buff.fillAligned();
        buff.setInt(p, (buff.length() - p) / Constants.FILE_BLOCK_SIZE);
    }
}
