/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.h2.jdbc.JdbcConnection;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.salesforce.apollo.consortium.EnqueuedTransaction;
import com.salesforce.apollo.state.ddl.ApolloDdlExecutor;
import com.salesforce.apollo.state.ddl.ApolloSchema;
import com.salesforce.apollo.state.ddl.ChainSchema;
import com.salesforce.apollo.state.h2.CdcSession;
import com.salesforce.apollo.state.h2.NullCapture;
import com.salesforce.apollo.state.jdbc.CdcConnection;

/**
 * @author hal.hildebrand
 *
 */
public class CdcEngine implements Function<EnqueuedTransaction, Message> {
    public static class Factory implements SchemaFactory {
        private final DataSource ds;

        private Factory(DataSource ds) {
            this.ds = ds;
        }

        @Override
        public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
            if (!(parentSchema instanceof ChainSchema)) {
                throw new IllegalArgumentException("Must be instance of " + ChainSchema.class.getCanonicalName());
            }
            return ApolloSchema.create((ChainSchema) parentSchema, name, operand, ds);
        }
    }

    private class DS implements DataSource {
        /**
         * The PrintWriter to which log messages should be directed.
         */
        private volatile PrintWriter logWriter = new PrintWriter(
                new OutputStreamWriter(System.out, StandardCharsets.UTF_8));

        @Override
        public Connection getConnection() throws SQLException {
            return connection;
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public int getLoginTimeout() throws SQLException {
            return 0;
        }

        @Override
        public PrintWriter getLogWriter() throws SQLException {
            return logWriter;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return iface != null && iface.isAssignableFrom(getClass());
        }

        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
        }

        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            this.logWriter = out;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("DS is not a wrapper.");
        }

    }

    private final CdcSession     capture;
    private Savepoint            checkpoint;
    private final JdbcConnection connection;
    private final DS             ds = new DS();
    private final Properties     jdbcProperties;
    private final String         jdbcUrl;
    private Capture              transaction;

    public CdcEngine(String url, Properties info) throws SQLException {
        connection = new JdbcConnection(url, info);
        capture = (CdcSession) connection.getSession();
        capture.setCdc(NullCapture.INSTANCE);
        this.jdbcUrl = url;
        this.jdbcProperties = info;
    }

    @Override
    public Message apply(EnqueuedTransaction t) {

        Connection connection = beginTransaction();
        Statement statement;
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            rollback();
            throw new IllegalStateException("Unable to create a statement", e);
        }
        if (transaction == null) {
            return null;
        }
        for (Any a : t.getTransaction().getBatchList()) {
            com.salesfoce.apollo.state.proto.Statement stmt;
            try {
                stmt = a.unpack(com.salesfoce.apollo.state.proto.Statement.class);
            } catch (InvalidProtocolBufferException e) {
                rollback();
                throw new IllegalStateException("Unable to deserialize batch statement", e);
            }
            try {
                statement.execute(stmt.getSql());
            } catch (SQLException e) {
                rollback();
                throw new IllegalStateException("Error processing statement: " + stmt.getSql(), e);
            }
        }
        return transaction.results();
    }

    public Connection beginTransaction() {
        transaction = new Capture();
        capture.setCdc(transaction);
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot set base connection to autocommit: false");
        }
        try {
            checkpoint = connection.setSavepoint();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot set savepoint for transaction");
        }
        return new CdcConnection(this, connection);
    }

    public JdbcConnection connect(String modelUri) throws SQLException {
        Properties config = new Properties();

        config.put(CalciteConnectionProperty.PARSER_FACTORY.camelName(), new SqlParserImplFactory() {
            @Override
            public DdlExecutor getDdlExecutor() {
                return new ApolloDdlExecutor();
            }

            @Override
            public SqlAbstractParserImpl getParser(Reader stream) {
                return SqlDdlParserImpl.FACTORY.getParser(stream);
            }
        });
        config.put(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(), "true");
        config.put(CalciteConnectionProperty.FUN.camelName(), "standard,oracle");
        config.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        return (JdbcConnection) DriverManager.getConnection("jdbc:calcite:", config);
    }

    public Connection getConnection() {
        return connection;
    }

    public DataSource getDatasource() {
        return ds;
    }

    public Capture getTransaction() {
        return transaction;
    }

    public JdbcConnection newConnection() {
        try {
            return new JdbcConnection(jdbcUrl, jdbcProperties);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot create new JDBC connection: " + jdbcUrl, e);
        }
    }

    public void rollback() {
        capture.setCdc(NullCapture.INSTANCE);
        transaction = null;
        try {
            connection.rollback(checkpoint);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot rollback to checkpoint for current transaction");
        }
    }
}
