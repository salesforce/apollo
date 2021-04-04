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

    private final Connection wrapped;

    public ConnectionWrapper(java.sql.Connection o) {
        wrapped = null;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return wrapped.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return wrapped.isWrapperFor(iface);
    }

    public Statement createStatement() throws SQLException {
        return wrapped.createStatement();
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return wrapped.prepareStatement(sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return wrapped.prepareCall(sql);
    }

    public String nativeSQL(String sql) throws SQLException {
        return wrapped.nativeSQL(sql);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        wrapped.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        return wrapped.getAutoCommit();
    }

    public void commit() throws SQLException {
        wrapped.commit();
    }

    public void rollback() throws SQLException {
        wrapped.rollback();
    }

    public void close() throws SQLException {
        wrapped.close();
    }

    public boolean isClosed() throws SQLException {
        return wrapped.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return wrapped.getMetaData();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        wrapped.setReadOnly(readOnly);
    }

    public boolean isReadOnly() throws SQLException {
        return wrapped.isReadOnly();
    }

    public void setCatalog(String catalog) throws SQLException {
        wrapped.setCatalog(catalog);
    }

    public String getCatalog() throws SQLException {
        return wrapped.getCatalog();
    }

    public void setTransactionIsolation(int level) throws SQLException {
        wrapped.setTransactionIsolation(level);
    }

    public int getTransactionIsolation() throws SQLException {
        return wrapped.getTransactionIsolation();
    }

    public SQLWarning getWarnings() throws SQLException {
        return wrapped.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        wrapped.clearWarnings();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return wrapped.createStatement(resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return wrapped.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return wrapped.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return wrapped.getTypeMap();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        wrapped.setTypeMap(map);
    }

    public void setHoldability(int holdability) throws SQLException {
        wrapped.setHoldability(holdability);
    }

    public int getHoldability() throws SQLException {
        return wrapped.getHoldability();
    }

    public Savepoint setSavepoint() throws SQLException {
        return wrapped.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return wrapped.setSavepoint(name);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        wrapped.rollback(savepoint);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        wrapped.releaseSavepoint(savepoint);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        return wrapped.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return wrapped.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return wrapped.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return wrapped.prepareStatement(sql, autoGeneratedKeys);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return wrapped.prepareStatement(sql, columnIndexes);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return wrapped.prepareStatement(sql, columnNames);
    }

    public Clob createClob() throws SQLException {
        return wrapped.createClob();
    }

    public Blob createBlob() throws SQLException {
        return wrapped.createBlob();
    }

    public NClob createNClob() throws SQLException {
        return wrapped.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return wrapped.createSQLXML();
    }

    public boolean isValid(int timeout) throws SQLException {
        return wrapped.isValid(timeout);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        wrapped.setClientInfo(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        wrapped.setClientInfo(properties);
    }

    public String getClientInfo(String name) throws SQLException {
        return wrapped.getClientInfo(name);
    }

    public Properties getClientInfo() throws SQLException {
        return wrapped.getClientInfo();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return wrapped.createArrayOf(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return wrapped.createStruct(typeName, attributes);
    }

    public void setSchema(String schema) throws SQLException {
        wrapped.setSchema(schema);
    }

    public String getSchema() throws SQLException {
        return wrapped.getSchema();
    }

    public void abort(Executor executor) throws SQLException {
        wrapped.abort(executor);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        wrapped.setNetworkTimeout(executor, milliseconds);
    }

    public int getNetworkTimeout() throws SQLException {
        return wrapped.getNetworkTimeout();
    }

    public void beginRequest() throws SQLException {
        wrapped.beginRequest();
    }

    public void endRequest() throws SQLException {
        wrapped.endRequest();
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey,
                                         int timeout) throws SQLException {
        return wrapped.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
        return wrapped.setShardingKeyIfValid(shardingKey, timeout);
    }

    public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
        wrapped.setShardingKey(shardingKey, superShardingKey);
    }

    public void setShardingKey(ShardingKey shardingKey) throws SQLException {
        wrapped.setShardingKey(shardingKey);
    }

}
