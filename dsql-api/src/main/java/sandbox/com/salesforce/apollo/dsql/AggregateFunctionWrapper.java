/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import java.sql.Connection;

import sandbox.java.lang.DJVM;
import sandbox.java.sql.SQLException;
import sandbox.org.h2.api.AggregateFunction;

/**
 * @author hal.hildebrand
 *
 */
public class AggregateFunctionWrapper {

    private final AggregateFunction wrapped;

    public AggregateFunctionWrapper(AggregateFunction wrapped) {
        this.wrapped = wrapped;
    }

    public void init(Connection conn) throws SQLException {
        sandbox.java.sql.Connection connection = new ConnectionWrapper(conn);
        wrapped.init(connection);
    }

    public int getType(int[] inputTypes) throws SQLException {
        return wrapped.getType(inputTypes);
    }

    public void add(Object value) throws SQLException {
        try {
            wrapped.add((sandbox.java.lang.Object) DJVM.sandbox(value));
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public Object getResult() throws SQLException {
        try {
            return DJVM.sandbox(wrapped.getResult());
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

}
