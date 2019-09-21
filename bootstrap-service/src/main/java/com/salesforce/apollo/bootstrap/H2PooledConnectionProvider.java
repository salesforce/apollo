/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.bootstrap;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.jdbcx.JdbcConnectionPool;
import org.jooq.ConnectionProvider;
import org.jooq.exception.DataAccessException;

/**
 * Because we all need pools.
 * 
 * @author hal.hildebrand
 * @since 222
 */
public class H2PooledConnectionProvider implements ConnectionProvider {
    private final JdbcConnectionPool pool;

    public H2PooledConnectionProvider(JdbcConnectionPool pool) {
        this.pool = pool;
    }

    @Override
    public Connection acquire() throws DataAccessException {
        try {
            Connection connection = pool.getConnection();
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException e) {
            throw new DataAccessException("cannot acquire connection from pool: " + pool, e);
        }
    }

    @Override
    public void release(Connection connection) throws DataAccessException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new DataAccessException("Unable to close pooled connection", e);
        }
    }

}
