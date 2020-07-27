/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.jdbc;

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

/**
 * @author hal.hildebrand
 *
 */
public class DelegatingConnection implements Connection {
    protected final Connection cdcConnection;

    public DelegatingConnection(Connection cdcConnection) {
        this.cdcConnection = cdcConnection;
    }

    public void abort(Executor arg0) throws SQLException {
        cdcConnection.abort(arg0);
    }

    public void beginRequest() throws SQLException {
        cdcConnection.beginRequest();
    }

    public void clearWarnings() throws SQLException {
        cdcConnection.clearWarnings();
    }

    public void close() throws SQLException {
        cdcConnection.close();
    }

    public void commit() throws SQLException {
        cdcConnection.commit();
    }

    public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
        return cdcConnection.createArrayOf(arg0, arg1);
    }

    public Blob createBlob() throws SQLException {
        return cdcConnection.createBlob();
    }

    public Clob createClob() throws SQLException {
        return cdcConnection.createClob();
    }

    public NClob createNClob() throws SQLException {
        return cdcConnection.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return cdcConnection.createSQLXML();
    }

    public Statement createStatement() throws SQLException {
        return cdcConnection.createStatement();
    }

    public Statement createStatement(int arg0, int arg1) throws SQLException {
        return cdcConnection.createStatement(arg0, arg1);
    }

    public Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {
        return cdcConnection.createStatement(arg0, arg1, arg2);
    }

    public Struct createStruct(String arg0, Object[] arg1) throws SQLException {
        return cdcConnection.createStruct(arg0, arg1);
    }

    public void endRequest() throws SQLException {
        cdcConnection.endRequest();
    }

    public boolean getAutoCommit() throws SQLException {
        return cdcConnection.getAutoCommit();
    }

    public String getCatalog() throws SQLException {
        return cdcConnection.getCatalog();
    }

    public Properties getClientInfo() throws SQLException {
        return cdcConnection.getClientInfo();
    }

    public String getClientInfo(String arg0) throws SQLException {
        return cdcConnection.getClientInfo(arg0);
    }

    public int getHoldability() throws SQLException {
        return cdcConnection.getHoldability();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return cdcConnection.getMetaData();
    }

    public int getNetworkTimeout() throws SQLException {
        return cdcConnection.getNetworkTimeout();
    }

    public String getSchema() throws SQLException {
        return cdcConnection.getSchema();
    }

    public int getTransactionIsolation() throws SQLException {
        return cdcConnection.getTransactionIsolation();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return cdcConnection.getTypeMap();
    }

    public SQLWarning getWarnings() throws SQLException {
        return cdcConnection.getWarnings();
    }

    public boolean isClosed() throws SQLException {
        return cdcConnection.isClosed();
    }

    public boolean isReadOnly() throws SQLException {
        return cdcConnection.isReadOnly();
    }

    public boolean isValid(int arg0) throws SQLException {
        return cdcConnection.isValid(arg0);
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return cdcConnection.isWrapperFor(arg0);
    }

    public String nativeSQL(String arg0) throws SQLException {
        return cdcConnection.nativeSQL(arg0);
    }

    public CallableStatement prepareCall(String arg0) throws SQLException {
        return cdcConnection.prepareCall(arg0);
    }

    public CallableStatement prepareCall(String arg0, int arg1, int arg2) throws SQLException {
        return cdcConnection.prepareCall(arg0, arg1, arg2);
    }

    public CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        return cdcConnection.prepareCall(arg0, arg1, arg2, arg3);
    }

    public PreparedStatement prepareStatement(String arg0) throws SQLException {
        return cdcConnection.prepareStatement(arg0);
    }

    public PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException {
        return cdcConnection.prepareStatement(arg0, arg1);
    }

    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2) throws SQLException {
        return cdcConnection.prepareStatement(arg0, arg1, arg2);
    }

    public PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException {
        return cdcConnection.prepareStatement(arg0, arg1, arg2, arg3);
    }

    public PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException {
        return cdcConnection.prepareStatement(arg0, arg1);
    }

    public PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException {
        return cdcConnection.prepareStatement(arg0, arg1);
    }

    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        cdcConnection.releaseSavepoint(arg0);
    }

    public void rollback() throws SQLException {
        cdcConnection.rollback();
    }

    public void rollback(Savepoint arg0) throws SQLException {
        cdcConnection.rollback(arg0);
    }

    public void setAutoCommit(boolean arg0) throws SQLException {
        cdcConnection.setAutoCommit(arg0);
    }

    public void setCatalog(String arg0) throws SQLException {
        cdcConnection.setCatalog(arg0);
    }

    public void setClientInfo(Properties arg0) throws SQLClientInfoException {
        cdcConnection.setClientInfo(arg0);
    }

    public void setClientInfo(String arg0, String arg1) throws SQLClientInfoException {
        cdcConnection.setClientInfo(arg0, arg1);
    }

    public void setHoldability(int arg0) throws SQLException {
        cdcConnection.setHoldability(arg0);
    }

    public void setNetworkTimeout(Executor arg0, int arg1) throws SQLException {
        cdcConnection.setNetworkTimeout(arg0, arg1);
    }

    public void setReadOnly(boolean arg0) throws SQLException {
        cdcConnection.setReadOnly(arg0);
    }

    public Savepoint setSavepoint() throws SQLException {
        return cdcConnection.setSavepoint();
    }

    public Savepoint setSavepoint(String arg0) throws SQLException {
        return cdcConnection.setSavepoint(arg0);
    }

    public void setSchema(String arg0) throws SQLException {
        cdcConnection.setSchema(arg0);
    }

    public void setShardingKey(ShardingKey shardingKey) throws SQLException {
        cdcConnection.setShardingKey(shardingKey);
    }

    public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
        cdcConnection.setShardingKey(shardingKey, superShardingKey);
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
        return cdcConnection.setShardingKeyIfValid(shardingKey, timeout);
    }

    public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey,
                                         int timeout) throws SQLException {
        return cdcConnection.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
    }

    public void setTransactionIsolation(int arg0) throws SQLException {
        cdcConnection.setTransactionIsolation(arg0);
    }

    public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
        cdcConnection.setTypeMap(arg0);
    }

    public <T> T unwrap(Class<T> arg0) throws SQLException {
        return cdcConnection.unwrap(arg0);
    }
}
