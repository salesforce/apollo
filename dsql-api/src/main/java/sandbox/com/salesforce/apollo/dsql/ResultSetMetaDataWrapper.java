/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import sandbox.java.lang.String;
import sandbox.java.sql.ResultSetMetaData;
import sandbox.java.sql.SQLException;

/**
 * @author hal.hildebrand
 *
 */
public class ResultSetMetaDataWrapper implements ResultSetMetaData {
    private final java.sql.ResultSetMetaData wrapper;

    public ResultSetMetaDataWrapper(java.sql.ResultSetMetaData wrapper) {
        this.wrapper = wrapper;
    }

    public String getCatalogName(int column) throws SQLException {
        try {
            return String.toDJVM(wrapper.getCatalogName(column));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getColumnClassName(int column) throws SQLException {
        try {
            return String.toDJVM(wrapper.getColumnClassName(column));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getColumnCount() throws SQLException {
        try {
            return wrapper.getColumnCount();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        try {
            return wrapper.getColumnDisplaySize(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getColumnLabel(int column) throws SQLException {
        try {
            return String.toDJVM(wrapper.getColumnLabel(column));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getColumnName(int column) throws SQLException {
        try {
            return String.toDJVM(wrapper.getColumnName(column));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getColumnType(int column) throws SQLException {
        try {
            return wrapper.getColumnType(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getColumnTypeName(int column) throws SQLException {
        try {
            return String.toDJVM(wrapper.getColumnTypeName(column));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getPrecision(int column) throws SQLException {
        try {
            return wrapper.getPrecision(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int getScale(int column) throws SQLException {
        try {
            return wrapper.getScale(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getSchemaName(int column) throws SQLException {
        try {
            return String.toDJVM(wrapper.getSchemaName(column));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getTableName(int column) throws SQLException {
        try {
            return String.toDJVM(wrapper.getTableName(column));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isAutoIncrement(int column) throws SQLException {
        try {
            return wrapper.isAutoIncrement(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isCaseSensitive(int column) throws SQLException {
        try {
            return wrapper.isCaseSensitive(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isCurrency(int column) throws SQLException {
        try {
            return wrapper.isCurrency(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        try {
            return wrapper.isDefinitelyWritable(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public int isNullable(int column) throws SQLException {
        try {
            return wrapper.isNullable(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isReadOnly(int column) throws SQLException {
        try {
            return wrapper.isReadOnly(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isSearchable(int column) throws SQLException {
        try {
            return wrapper.isSearchable(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isSigned(int column) throws SQLException {
        try {
            return wrapper.isSigned(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        try {
            return wrapper.isWrapperFor(iface);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean isWritable(int column) throws SQLException {
        try {
            return wrapper.isWritable(column);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
