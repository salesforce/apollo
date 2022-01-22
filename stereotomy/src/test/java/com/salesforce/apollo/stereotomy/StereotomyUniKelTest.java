/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.sql.SQLException;
import java.util.Properties;

import org.h2.jdbc.JdbcConnection;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.db.UniKERLDirect;

import liquibase.Liquibase;
import liquibase.database.core.H2Database;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * @author hal.hildebrand
 *
 */
public class StereotomyUniKelTest extends StereotomyTests {

    void initializeKel() throws SQLException, LiquibaseException {
        final var url = String.format("jdbc:h2:mem:test_engine-%s;DB_CLOSE_DELAY=-1", Math.random());
        var connection = new JdbcConnection(url, new Properties(), "", "", false);

        var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
        try (Liquibase liquibase = new Liquibase("/stereotomy/initialize.xml", new ClassLoaderResourceAccessor(),
                                                 database)) {
            liquibase.update((String) null);
        }
        connection = new JdbcConnection(url, new Properties(), "", "", false);
        kel = new UniKERLDirect(connection, DigestAlgorithm.DEFAULT);
    }
}
