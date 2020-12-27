/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;

/**
 * @author hal.hildebrand
 *
 */
public class TransformTest {

    @Test
    public void smokin() throws Exception {
        Config sqlParserConfig = SqlParser.configBuilder()
                                          .setParserFactory(SqlDdlParserImpl.FACTORY)
                                          .setConformance(SqlConformanceEnum.BABEL)
                                          .build();

        String sql = "create table s.books (id int, title varchar(50), author varchar(50), price float, qty int)";
        SqlParser parser = SqlParser.create(sql, sqlParserConfig);

        SqlNode exp = parser.parseStmtList();
        
        System.out.println(exp);

        try (Connection connection = connect()) {

            connection.setAutoCommit(false);
            Statement s = connection.createStatement();

            s.execute("create schema s");
            s.execute("create table s.books (id int, title varchar(50), author varchar(50), price float, qty int)");

            s.execute("insert into s.books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)");
            s.execute("insert into s.books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)");
            s.execute("insert into s.books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)");
            s.execute("insert into s.books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)");
            s.execute("insert into s.books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");

            try (ResultSet r = s.executeQuery("select id, title from s.books")) {
                assertThat(r.next(), is(true));
                assertThat(r.getInt(1), is(1001));
                assertThat(r.next(), is(true));
            }
        }
    }

    private Connection connect() throws SQLException {
        Updater engine = new Updater("jdbc:h2:mem:test_mem", new Properties(), new File("target/chkpoints", "tst"));
        return engine.newConnection();
    }
}
