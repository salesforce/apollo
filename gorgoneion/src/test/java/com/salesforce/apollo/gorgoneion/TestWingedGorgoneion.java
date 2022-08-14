/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.gorgoneion;

import java.security.SecureRandom;
import java.util.Properties;

import org.h2.jdbc.JdbcConnection;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.gorgoneion.proto.UsernamePassword;
import com.salesforce.apollo.gorgoneion.db.SqlVault;

import liquibase.Liquibase;
import liquibase.database.core.H2Database;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * @author hal.hildebrand
 *
 */
public class TestWingedGorgoneion {

    @Test
    public void smokin() throws Exception {
        var secureRandom = SecureRandom.getInstance("SHA1PRNG");
        secureRandom.setSeed(new byte[] { 6, 6, 6 });

        final var url = "jdbc:h2:mem:test_gorgoneion-smoke;DB_CLOSE_DELAY=-1";
        var connection = new JdbcConnection(url, new Properties(), "", "", false);

        var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
        try (Liquibase liquibase = new Liquibase("/gorgoneion/gorgoneion.xml", new ClassLoaderResourceAccessor(),
                                                 database)) {
            liquibase.update((String) null);
        }
        connection = new JdbcConnection(url, new Properties(), "", "", false);

        var gorgoneion = new WingedGorgoneion(new SqlVault(DSL.using(connection, SQLDialect.H2), secureRandom),
                                              secureRandom);

        final var up = UsernamePassword.newBuilder()
                                       .setName("J.R. Bob Dobbs")
                                       .setPassword("Give me food, or give me Slack, or KILL me")
                                       .build();
        gorgoneion.createUser(up);
    }
}
