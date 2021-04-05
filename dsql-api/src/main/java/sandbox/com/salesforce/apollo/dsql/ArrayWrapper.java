/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import static sandbox.com.salesforce.apollo.dsql.ConnectionWrapper.convertClassMap;

import java.util.Map;

import sandbox.java.lang.DJVM;
import sandbox.java.lang.Object;
import sandbox.java.lang.String;
import sandbox.java.sql.Array;
import sandbox.java.sql.ResultSet;
import sandbox.java.sql.SQLException;

/**
 * @author hal.hildebrand
 *
 */
public class ArrayWrapper implements Array {
    private final java.sql.Array wrapped;

    public ArrayWrapper(java.sql.Array wrapped) {
        this.wrapped = wrapped;
    }

    public void free() throws SQLException {
        try {
            wrapped.free();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getArray() throws SQLException {
        try {
            return (Object) DJVM.sandbox(wrapped.getArray());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public Object getArray(long index, int count) throws SQLException {
        try {
            return (Object) DJVM.sandbox(wrapped.getArray(index, count));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        try {
            return (Object) DJVM.sandbox(wrapped.getArray(index, count, convertClassMap(map)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        try {
            return (Object) DJVM.sandbox(wrapped.getArray(convertClassMap(map)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        } catch (ClassNotFoundException e) {
            throw DJVM.toRuntimeException(e);
        }
    }

    public int getBaseType() throws SQLException {
        try {
            return wrapped.getBaseType();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getBaseTypeName() throws SQLException {
        try {
            return String.toDJVM(wrapped.getBaseTypeName());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getResultSet() throws SQLException {
        try {
            return new ResultSetWrapper(wrapped.getResultSet());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getResultSet(long index, int count) throws SQLException {
        try {
            return new ResultSetWrapper(wrapped.getResultSet(index, count));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        try {
            return new ResultSetWrapper(wrapped.getResultSet(index, count, convertClassMap(map)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        try {
            return new ResultSetWrapper(wrapped.getResultSet(convertClassMap(map)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public java.sql.Array toJsArray() {
        return wrapped;
    }
}
