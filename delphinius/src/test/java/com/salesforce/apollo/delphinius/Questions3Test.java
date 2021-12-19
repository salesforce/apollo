/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import java.util.Properties;

import org.h2.jdbc.JdbcConnection;
import org.jooq.impl.DSL;
import org.junit.Test;

import com.salesforce.apollo.delphinius.schema.tables.Subject;

import static com.salesforce.apollo.delphinius.schema.tables.Edge.*;
import static com.salesforce.apollo.delphinius.schema.tables.Namespace.*;
import static com.salesforce.apollo.delphinius.schema.tables.Relation.*;
import static com.salesforce.apollo.delphinius.schema.tables.Subject.*;

import liquibase.Liquibase;
import liquibase.database.core.H2Database;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * @author hal.hildebrand
 *
 */
public class Questions3Test {

    @Test
    public void smokin() throws Exception {
        final var url = "jdbc:h2:mem:test_engine-smoke;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";
        var connection = new JdbcConnection(url, new Properties(), "", "");

        var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
        try (Liquibase liquibase = new Liquibase("/delphinius.xml", new ClassLoaderResourceAccessor(), database)) {
            liquibase.update((String) null);
        }
        connection = new JdbcConnection(url, new Properties(), "", "");

        Oracle oracle = new Oracle(connection);
        oracle.addTuple("foo", "HelpDesk", "member", "Admins");
        oracle.addTuple("foo", "Ali", "member", "Admins");
        oracle.addTuple("foo", "Ali", "member", "Users");
        oracle.addTuple("foo", "Burcu", "member", "Users");
        oracle.addTuple("foo", "Can", "member", "Users");
        oracle.addTuple("foo", "Managers", "member", "Users");
        oracle.addTuple("foo", "Technicians", "member", "Users");
        oracle.addTuple("foo", "Demet", "member", "HelpDesk");
        oracle.addTuple("foo", "Egin", "member", "HelpDesk");
        oracle.addTuple("foo", "Egin", "member", "Users");
        oracle.addTuple("foo", "Fuat", "member", "Managers");
        oracle.addTuple("foo", "G l", "member", "Managers");
        oracle.addTuple("foo", "Hakan", "member", "Technicians");
        oracle.addTuple("foo", "Irmak", "member", "Technicians");
        oracle.addTuple("foo", "ABCTechnicians", "member", "Technicians");
        oracle.addTuple("foo", "Jale", "member", "ABCTechnicians");

        var dsl = DSL.using(connection);
        Subject pa = SUBJECT.as("parent");
        Subject ch = SUBJECT.as("child");
        System.out.println(dsl.select(pa.NAME.as("parent"), ch.NAME.as("child"), EDGE.HOPS).from(pa, ch).join(EDGE)
                              .on(EDGE.PARENT.eq(pa.ID).and(EDGE.CHILD.eq(ch.ID))).fetch());
    }
}
