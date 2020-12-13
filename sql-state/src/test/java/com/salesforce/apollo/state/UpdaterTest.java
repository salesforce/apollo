/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesfoce.apollo.state.proto.Results;
import com.salesforce.apollo.consortium.EnqueuedTransaction;
import com.salesforce.apollo.protocols.HashKey;

/**
 * @author hal.hildebrand
 *
 */
public class UpdaterTest {

    @Test
    public void smoke() throws Exception {
        CdcEngine engine = new CdcEngine("jdbc:h2:mem:test_update", new Properties());
        Updater updater = new Updater(engine);

        Connection connection = engine.newConnection();

        Statement statement = connection.createStatement();
        statement.execute("create table books (id int, title varchar(50), author varchar(50), price float, qty int,  primary key (id))");

        Transaction.Builder builder = Transaction.newBuilder();
        builder.addBatch(batch("insert into books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)"));
        builder.addBatch(batch("insert into books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)"));
        builder.addBatch(batch("insert into books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)"));
        builder.addBatch(batch("insert into books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)"));
        builder.addBatch(batch("insert into books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)"));
        Transaction transaction = builder.build();

        engine.apply(new EnqueuedTransaction(HashKey.ORIGIN, transaction));
        Capture capture = engine.getTransaction();
        assertNotNull(capture);
        assertEquals(5, capture.getChanges().size());

        Results results = capture.results();
        assertNotNull(results);
        assertEquals(5, results.getResultsCount());

        engine.rollback();

        ResultSet books = statement.executeQuery("select * from books");
        assertFalse(books.first());
        
        updater.begin();
        updater.accept(ExecutedTransaction.newBuilder().setResult(Any.pack(results)).build());
        updater.complete();
        
        books = statement.executeQuery("select * from books");
        assertTrue(books.first());
        for (int i = 0; i < 4; i++) {
            assertTrue(books.next(), "Missing row: " + (i + 1));
        }
    }

    private Any batch(String sql) {
        return Any.pack(com.salesfoce.apollo.state.proto.Statement.newBuilder().setSql(sql).build());
    }
}
