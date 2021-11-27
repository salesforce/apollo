/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.utils;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.ShardingKey;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class DelegatingJdbcConnector implements Connection {
    private final Connection wrapped;

    public DelegatingJdbcConnector(Connection wrapped) {
        this.wrapped = wrapped;
    }

    public void abort(Executor executor) throws SQLException {
        wrapped.abort(executor);
    }

    public void beginRequest() throws SQLException {
        wrapped.beginRequest();
    }

    public void clearWarnings() throws SQLException {
        wrapped.clearWarnings();
    }

    public void close() throws SQLException {
        wrapped.close();
    }

    public void commit() throws SQLException {
        wrapped.commit();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return wrapped.createArrayOf(typeName, elements);
    }

    public Blob createBlob() throws SQLException {
        return wrapped.createBlob();
    }

    public Clob createClob() throws SQLException {
        return wrapped.createClob();
    }

    public NClob createNClob() throws SQLException {
        return wrapped.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return wrapped.createSQLXML();
    }

    public Statement createStatement() throws SQLException {
        return wrapped.createStatement();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return wrapped.createStatement(resultSetType, resultSetConcurrency);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        return wrapped.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return wrapped.createStruct(typeName, attributes);
    }

    public void endRequest() throws SQLException {
        wrapped.endRequest();
    }

    public boolean getAutoCommit() throws SQLException {
        return wrapped.getAutoCommit();
    }

    public String getCatalog() throws SQLException {
        return wrapped.getCatalog();
    }

    public Properties getClientInfo() throws SQLException {
        return wrapped.getClientInfo();
    }

    public String getClientInfo(String name) throws SQLException {
        return wrapped.getClientInfo(name);
    }

    public int getHoldability() throws SQLException {
        return wrapped.getHoldability();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return wrapped.getMetaData();
    }

    public int getNetworkTimeout() throws SQLException {
        return wrapped.getNetworkTimeout();
    }

    public String getSchema() throws SQLException {
        return wrapped.getSchema();
    }

    public int getTransactionIsolation() throws SQLException {
        return wrapped.getTransactionIsolation();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return wrapped.getTypeMap();
    }

    public SQLWarning getWarnings() throws SQLException {
        return wrapped.getWarnings();
    }

    public boolean isClosed() throws SQLException {
        return wrapped.isClosed();
    }

    public boolean isReadOnly() throws SQLException {
        return wrapped.isReadOnly();
    }

    public boolean isValid(int timeout) throws SQLException {
        return wrapped.isValid(timeout);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return wrapped.isWrapperFor(iface);
    }

    public String nativeSQL(String sql) throws SQLException {
        return wrapped.nativeSQL(sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return wrapped.prepareCall(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return wrapped.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return wrapped.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return wrapped.prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return wrapped.prepareStatement(sql, autoGeneratedKeys);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return wrapped.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        return wrapped.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return wrapped.prepareStatement(sql, columnIndexes);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return wrapped.prepareStatement(sql, columnNames);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        wrapped.releaseSavepoint(savepoint);
    }

    public void rollback() throws SQLException {
        wrapped.rollback();
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        wrapped.rollback(savepoint);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        wrapped.setAutoCommit(autoCommit);
    }

    public void setCatalog(String catalog) throws SQLException {
        wrapped.setCatalog(catalog);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        wrapped.setClientInfo(properties);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        wrapped.setClientInfo(name, value);
    }

    public void setHoldability(int holdability) throws SQLException {
        wrapped.setHoldability(holdability);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        wrapped.setNetworkTimeout(executor, milliseconds);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        wrapped.setReadOnly(readOnly);
    }

    public Savepoint setSavepoint() throws SQLException {
        return wrapped.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return wrapped.setSavepoint(name);
    }

    public void setSchema(String schema) throws SQLException {
        wrapped.setSchema(schema);
    }

    public void setShardingKey(ShardingKey shardingKey) throws SQLException {
        wrapped.setShardingKey(shardingKey);
    }

    public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
        wrapped.setShardingKey(shardingKey, superShardingKey);
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
        return wrapped.setShardingKeyIfValid(shardingKey, timeout);
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey,
                                         int timeout) throws SQLException {
        return wrapped.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
    }

    public void setTransactionIsolation(int level) throws SQLException {
        wrapped.setTransactionIsolation(level);
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        wrapped.setTypeMap(map);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return wrapped.unwrap(iface);
    }
}
