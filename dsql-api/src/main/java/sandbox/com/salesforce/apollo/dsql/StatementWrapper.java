/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import sandbox.java.lang.DJVM;
import sandbox.java.lang.String;
import sandbox.java.sql.Connection;
import sandbox.java.sql.ResultSet;
import sandbox.java.sql.SQLException;
import sandbox.java.sql.SQLWarning;
import sandbox.java.sql.Statement;

/**
 * @author hal.hildebrand
 *
 */
public class StatementWrapper implements Statement {
    private final Connection         connection;
    private final java.sql.Statement wrapper;

    public StatementWrapper(Connection connection, java.sql.Statement wrapper) {
        this.connection = connection;
        this.wrapper = wrapper;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            return new ResultSetWrapper(wrapper.executeQuery(String.fromDJVM(sql)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            return wrapper.executeUpdate(String.fromDJVM(sql));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        try {
            return wrapper.getMaxFieldSize();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        try {
            wrapper.setMaxFieldSize(max);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getMaxRows() throws SQLException {
        try {
            return wrapper.getMaxRows();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        try {
            wrapper.setMaxRows(max);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        try {
            wrapper.setEscapeProcessing(enable);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        try {
            return wrapper.getQueryTimeout();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        try {
            wrapper.setQueryTimeout(seconds);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void cancel() throws SQLException {
        try {
            wrapper.cancel();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        try {
            return new SQLWarning(wrapper.getWarnings());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void clearWarnings() throws SQLException {
        try {
            wrapper.clearWarnings();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        try {
            wrapper.setCursorName(String.fromDJVM(name));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            return wrapper.execute(String.fromDJVM(sql));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        try {
            return new ResultSetWrapper(wrapper.getResultSet());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getUpdateCount() throws SQLException {
        try {
            return wrapper.getUpdateCount();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        try {
            return wrapper.getMoreResults();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        try {
            wrapper.setFetchDirection(direction);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        try {
            return wrapper.getFetchDirection();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        try {
            wrapper.setFetchSize(rows);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        try {
            return wrapper.getFetchSize();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        try {
            return wrapper.getResultSetConcurrency();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getResultSetType() throws SQLException {
        try {
            return wrapper.getResultSetType();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        try {
            wrapper.addBatch(String.fromDJVM(sql));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        try {
            wrapper.clearBatch();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int[] executeBatch() throws SQLException {
        try {
            return wrapper.executeBatch();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        try {
            return wrapper.getMoreResults(current);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        try {
            return new ResultSetWrapper(wrapper.getGeneratedKeys());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            return wrapper.executeUpdate(String.fromDJVM(sql), autoGeneratedKeys);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            return wrapper.executeUpdate(String.fromDJVM(sql), columnIndexes);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            return wrapper.executeUpdate(String.fromDJVM(sql), (java.lang.String[]) DJVM.unsandbox(columnNames));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            return wrapper.execute(String.fromDJVM(sql), autoGeneratedKeys);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        try {
            return wrapper.execute(String.fromDJVM(sql), columnIndexes);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        try {
            return wrapper.execute(String.fromDJVM(sql), (java.lang.String[]) DJVM.unsandbox(columnNames));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        try {
            return wrapper.getResultSetHoldability();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        try {
            return wrapper.getLargeUpdateCount();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        try {
            wrapper.setLargeMaxRows(max);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        try {
            return wrapper.getLargeMaxRows();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        try {
            return wrapper.executeLargeBatch();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        try {
            return wrapper.executeLargeUpdate(String.fromDJVM(sql));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            return wrapper.executeLargeUpdate(String.fromDJVM(sql), autoGeneratedKeys);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        try {
            return wrapper.executeLargeUpdate(String.fromDJVM(sql), columnIndexes);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        try {
            return wrapper.executeLargeUpdate(String.fromDJVM(sql), (java.lang.String[]) DJVM.unsandbox(columnNames));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String enquoteLiteral(String val) throws SQLException {
        try {
            return String.toDJVM(wrapper.enquoteLiteral(String.fromDJVM(val)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String enquoteIdentifier(String identifier, boolean alwaysQuote) throws SQLException {
        try {
            return String.toDJVM(wrapper.enquoteIdentifier(String.fromDJVM(identifier), alwaysQuote));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isSimpleIdentifier(String identifier) throws SQLException {
        try {
            return wrapper.isSimpleIdentifier(String.fromDJVM(identifier));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public String enquoteNCharLiteral(String val) throws SQLException {
        try {
            return String.toDJVM(wrapper.enquoteNCharLiteral(String.fromDJVM(val)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

}
