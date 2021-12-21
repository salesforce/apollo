/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.Trigger;

/**
 * @author hal.hildebrand
 *
 */
public class DomainMaintenance implements Trigger {

    @SuppressWarnings("unused")
    private String type;

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
    }

    @Override
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before,
                     int type) throws SQLException {
        assert !before || type != DELETE : "this is an after delete trigger";
        this.type = switch (tableName.toLowerCase()) {
        case "object" -> Oracle.OBJECT_TYPE;
        case "relation" -> Oracle.RELATION_TYPE;
        case "subject" -> Oracle.SUBJECT_TYPE;
        default -> throw new IllegalArgumentException("Unexpected value: " + tableName.toLowerCase());
        };
    }

}
