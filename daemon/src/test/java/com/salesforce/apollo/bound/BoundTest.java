/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.bound;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/**
 * @author hal.hildebrand
 *
 */
public class BoundTest {

    @Test
    public void smoke() {
        List<String> whitelist = Arrays.asList("org.h2.api.Trigger", "java.sql.Array", "java.sql.BatchUpdateException",
                                               "java.sql.Blob", "java.sql.CallableStatement",
                                               "java.sql.ClientInfoStatus", "java.sql.Clob", "java.sql.Connection",
                                               "java.sql.ConnectionBuilder", "java.sql.DatabaseMetaData",
                                               "java.sql.DataTruncation", "java.sql.Date",
                                               "java.sql.DriverPropertyInfo", "java.sql.JDBCType", "java.sql.NClob",
                                               "java.sql.ParameterMetaData", "java.sql.PreparedStatement",
                                               "java.sql.PseudoColumnUsage", "java.sql.Ref", "java.sql.ResultSet",
                                               "java.sql.ResultSetMetaData", "java.sql.RowId", "java.sql.RowIdLifetime",
                                               "java.sql.Savepoint", "java.sql.ShardingKey",
                                               "java.sql.ShardingKeyBuilder", "java.sql.SQLClientInfoException",
                                               "java.sql.SQLData", "java.sql.SQLDataException", "java.sql.SQLException",
                                               "java.sql.SQLFeatureNotSupportedException", "java.sql.SQLInput",
                                               "java.sql.SQLIntegrityConstraintViolationException",
                                               "java.sql.SQLInvalidAuthorizationSpecException",
                                               "java.sql.SQLNonTransientConnectionException",
                                               "java.sql.SQLNonTransientException", "java.sql.SQLOutput",
                                               "java.sql.SQLPermission", "java.sql.SQLRecoverableException",
                                               "java.sql.SQLSyntaxErrorException", "java.sql.SQLTimeoutException",
                                               "java.sql.SQLTransactionRollbackException",
                                               "java.sql.SQLTransientConnectionException",
                                               "java.sql.SQLTransientException", "java.sql.SQLType",
                                               "java.sql.SQLWarning", "java.sql.SQLXML", "java.sql.Statement",
                                               "java.sql.Struct", "java.sql.Time", "java.sql.Timestamp",
                                               "java.sql.Types", "java.sql.Wrapper")
                                       .stream()
                                       .collect(Collectors.toList());
        Bound sqlBound = new Bound("SQL", null, whitelist, null);
    }
}
