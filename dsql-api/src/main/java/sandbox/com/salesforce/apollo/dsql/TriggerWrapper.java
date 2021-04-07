/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import java.sql.Connection;

import sandbox.java.lang.DJVM; 
import sandbox.java.lang.String;
import sandbox.java.sql.SQLException;
import sandbox.org.h2.api.Trigger;

/**
 * @author hal.hildebrand
 *
 */
final public class TriggerWrapper {
    private final Trigger trigger;

    public TriggerWrapper(java.lang.Object trigger) {
        this.trigger = (Trigger) trigger;
    }

    public void init() {

    }

    public void init(Connection conn, java.lang.String schemaName, java.lang.String triggerName,
                     java.lang.String tableName, boolean before, int type) throws SQLException {
        ConnectionWrapper wrapped = new ConnectionWrapper(conn);
        trigger.init(wrapped, String.toDJVM(schemaName), String.toDJVM(triggerName), String.toDJVM(tableName), before,
                     type);
    }

    public void fire(Connection conn,  Object[] oldRow,  Object[] newRow) throws SQLException {
        ConnectionWrapper wrapped = new ConnectionWrapper(conn);
        try {
            java.lang.Object sandboxed = DJVM.sandbox(oldRow);
            trigger.fire(wrapped, (Object[]) sandboxed, (Object[]) DJVM.sandbox(newRow));
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public void close() throws SQLException {
        trigger.close();
    }

    public void remove() throws SQLException {
        trigger.remove();
    }

}
