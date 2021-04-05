/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import sandbox.java.lang.String;
import sandbox.java.sql.SQLException;
import sandbox.java.sql.Savepoint;

/**
 * @author hal.hildebrand
 *
 */
public class SavepointWrapper implements Savepoint {

    private final java.sql.Savepoint wrapped;

    public SavepointWrapper(java.sql.Savepoint wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public int getSavepointId() throws SQLException {
        try {
            return wrapped.getSavepointId();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String getSavepointName() throws SQLException {
        try {
            return String.toDJVM(wrapped.getSavepointName());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }
    
    java.sql.Savepoint getWrapped() {
        return wrapped;
    }

}
