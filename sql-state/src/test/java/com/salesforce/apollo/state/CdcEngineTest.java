/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;

/**
 * @author hal.hildebrand
 *
 */
public class CdcEngineTest {

    @Test
    public void smoke() throws Exception {
        CdcEngine engine = new CdcEngine("jdbc:h2:mem:test_mem", new Properties());

        Connection connection = engine.beginTransaction();

        Statement statement = connection.createStatement();
        statement.execute("create table books (id int, title varchar(50), author varchar(50), price float, qty int,  primary key (id))");

        statement.execute("insert into books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)");
        statement.execute("insert into books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)");
        statement.execute("insert into books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)");
        statement.execute("insert into books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)");
        statement.execute("insert into books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");

        List<Transaction> transactions = engine.getTransactions();

        assertEquals(1, transactions.size());
        Transaction transaction = transactions.get(0);

        assertEquals(5, transaction.getChanges().size());

        byte[] serialized = transaction.serialize();

        assertEquals(228, serialized.length);

    }
}
