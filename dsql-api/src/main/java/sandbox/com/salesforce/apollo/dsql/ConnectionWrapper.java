/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import sandbox.java.lang.DJVM;
import sandbox.java.lang.Object;
import sandbox.java.lang.String;
import sandbox.java.sql.Array;
import sandbox.java.sql.Blob;
import sandbox.java.sql.CallableStatement;
import sandbox.java.sql.Clob;
import sandbox.java.sql.Connection;
import sandbox.java.sql.DatabaseMetaData;
import sandbox.java.sql.NClob;
import sandbox.java.sql.PreparedStatement;
import sandbox.java.sql.SQLClientInfoException;
import sandbox.java.sql.SQLException;
import sandbox.java.sql.SQLWarning;
import sandbox.java.sql.SQLXML;
import sandbox.java.sql.Savepoint;
import sandbox.java.sql.ShardingKey;
import sandbox.java.sql.Statement;
import sandbox.java.sql.Struct;

/**
 * @author hal.hildebrand
 *
 */
public class ConnectionWrapper implements Connection {

    private final java.sql.Connection wrapped;

    public ConnectionWrapper(java.sql.Connection o) {
        wrapped = null;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Statement createStatement() throws SQLException {
        try {
            return new StatementWrapper(this, wrapped.createStatement());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        try {
            return new PreparedStatementWrapper(this, wrapped.prepareStatement(String.fromDJVM(sql)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        try {
            return new CallableStatementWrapper(this, wrapped.prepareCall(String.fromDJVM(sql)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String nativeSQL(String sql) throws SQLException {
        try {
            return String.toDJVM(wrapped.nativeSQL(String.fromDJVM(sql)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
    }

    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    public void commit() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void rollback() throws SQLException {
        try {
            wrapped.rollback();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void close() throws SQLException {
    }

    public boolean isClosed() throws SQLException {
        return false;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        try {
            return new DatabaseMetadataWrapper(wrapped.getMetaData());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isReadOnly() throws SQLException {
        try {
            return wrapped.isReadOnly();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setCatalog(String catalog) throws SQLException {
        try {
            wrapped.setCatalog(String.fromDJVM(catalog));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getCatalog() throws SQLException {
        try {
            return String.toDJVM(wrapped.getCatalog());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getTransactionIsolation() throws SQLException {
        try {
            return wrapped.getTransactionIsolation();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public SQLWarning getWarnings() throws SQLException {
        try {
            return new SQLWarning(wrapped.getWarnings());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void clearWarnings() throws SQLException {
        try {
            wrapped.clearWarnings();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            return new StatementWrapper(this, wrapped.createStatement(resultSetType, resultSetConcurrency));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        try {
            return new PreparedStatementWrapper(this,
                    wrapped.prepareStatement(String.fromDJVM(sql), resultSetType, resultSetConcurrency));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            return new CallableStatementWrapper(this,
                    wrapped.prepareCall(String.fromDJVM(sql), resultSetType, resultSetConcurrency));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getHoldability() throws SQLException {
        try {
            return wrapped.getHoldability();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Savepoint setSavepoint() throws SQLException {
        try {
            return wrapped.setSavepoint();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        try {
            return wrapped.setSavepoint(name);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        try {
            wrapped.rollback(savepoint);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        try {
            wrapped.releaseSavepoint(savepoint);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        try {
            return new StatementWrapper(this,
                    wrapped.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        try {
            return new PreparedStatementWrapper(this,
                    wrapped.prepareStatement(String.fromDJVM(sql), resultSetType, resultSetConcurrency,
                                             resultSetHoldability));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        try {
            return new CallableStatementWrapper(this, wrapped.prepareCall(String.fromDJVM(sql), resultSetType,
                                                                          resultSetConcurrency, resultSetHoldability));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        try {
            return new PreparedStatementWrapper(this,
                    wrapped.prepareStatement(String.fromDJVM(sql), autoGeneratedKeys));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        try {
            return new PreparedStatementWrapper(this, wrapped.prepareStatement(String.fromDJVM(sql), columnIndexes));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        try {
            return new PreparedStatementWrapper(this,
                    wrapped.prepareStatement(String.fromDJVM(sql), (java.lang.String[]) DJVM.unsandbox(columnNames)));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isValid(int timeout) throws SQLException {
        return true;
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException();
    }

    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        try {
            return wrapped.createArrayOf(typeName, elements);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        try {
            return wrapped.createStruct(String.fromDJVM(typeName), attributes);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setSchema(String schema) throws SQLException {
        try {
            wrapped.setSchema(String.fromDJVM(schema));
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public String getSchema() throws SQLException {
        try {
            return String.toDJVM(wrapped.getSchema());
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void abort(Executor executor) throws SQLException {
        try {
            wrapped.abort(executor);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    }

    public int getNetworkTimeout() throws SQLException {
        try {
            return wrapped.getNetworkTimeout();
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void beginRequest() throws SQLException {
    }

    public void endRequest() throws SQLException {
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey,
                                         int timeout) throws SQLException {
        try {
            return wrapped.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
        try {
            return wrapped.setShardingKeyIfValid(shardingKey, timeout);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
        try {
            wrapped.setShardingKey(shardingKey, superShardingKey);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

    public void setShardingKey(ShardingKey shardingKey) throws SQLException {
        try {
            wrapped.setShardingKey(shardingKey);
        } catch (java.sql.SQLException e) {
            throw new SQLException(e);
        }
    }

}
