/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import java.util.Map;

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

    public String getBaseTypeName() throws SQLException {
        try {
            return wrapped.getBaseTypeName();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getBaseType() throws SQLException {
        try {
            return wrapped.getBaseType();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getArray() throws SQLException {
        try {
            return wrapped.getArray();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        try {
            return wrapped.getArray(map);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getArray(long index, int count) throws SQLException {
        try {
            return wrapped.getArray(index, count);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        try {
            return wrapped.getArray(index, count, map);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getResultSet() throws SQLException {
        try {
            return wrapped.getResultSet();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        try {
            return wrapped.getResultSet(map);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getResultSet(long index, int count) throws SQLException {
        try {
            return wrapped.getResultSet(index, count);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        try {
            return wrapped.getResultSet(index, count, map);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void free() throws SQLException {
        try {
            wrapped.free();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }
}
