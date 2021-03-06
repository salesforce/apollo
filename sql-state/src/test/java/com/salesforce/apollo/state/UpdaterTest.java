/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static com.salesforce.apollo.state.Mutator.batch;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.google.protobuf.Any;
import com.salesfoce.apollo.consortium.proto.ExecutedTransaction;
import com.salesfoce.apollo.consortium.proto.Transaction;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public class UpdaterTest {

    @Test
    public void smoke() throws Exception {
        SqlStateMachine updater = new SqlStateMachine("jdbc:h2:mem:test_update", new Properties(),
                new File("target/chkpoints"));
        updater.getExecutor().beginBlock(0, DigestAlgorithm.DEFAULT.getLast());
        updater.initializeEvents();

        Connection connection = updater.newConnection();

        Statement statement = connection.createStatement();
        statement.execute("create table books (id int, title varchar(50), author varchar(50), price float, qty int,  primary key (id))");

        Transaction.Builder builder = Transaction.newBuilder();
        builder.setTxn(Any.pack(batch("insert into books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)",
                                      "insert into books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)",
                                      "insert into books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)",
                                      "insert into books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)",
                                      "insert into books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)")));
        Transaction transaction = builder.build();

        updater.getExecutor()
               .execute(DigestAlgorithm.DEFAULT.getOrigin(),
                        ExecutedTransaction.newBuilder()
                                           .setHash(Digest.NONE.toDigeste())
                                           .setTransaction(transaction)
                                           .build(),
                        null);

        ResultSet books = statement.executeQuery("select * from books");
        assertTrue(books.first());
        for (int i = 0; i < 4; i++) {
            assertTrue(books.next(), "Missing row: " + (i + 1));
        }
    }

    @Test
    public void eventPublishing() throws Exception {
        String json = "{\"customer_name\": \"John\", \"items\": { \"description\": \"milk\", \"quantity\": 4 } }";

        SqlStateMachine updater = new SqlStateMachine("jdbc:h2:mem:test_publish", new Properties(),
                new File("target/chkpoints"));
        updater.getExecutor().beginBlock(0, DigestAlgorithm.DEFAULT.getLast());
        updater.initializeEvents();

        Connection connection = updater.newConnection();
        SqlStateMachine.publish(connection, "test", json);
        connection.commit();
        Statement statement = connection.createStatement();
        ResultSet events = statement.executeQuery("select * from __APOLLO_INTERNAL__.TRAMPOLINE");

        assertTrue(events.next());
//        System.out.println(events.getInt(1) + " : " + events.getString(2) + " : " + events.getString(3));
        assertFalse(events.next());

        CallableStatement call = connection.prepareCall("call __APOLLO_INTERNAL__.PUBLISH(?1, ?2)");
        call.setString(1, "test");
        call.setString(2, json);
        call.execute();
        events = statement.executeQuery("select * from __APOLLO_INTERNAL__.TRAMPOLINE");
        assertTrue(events.next());
//        System.out.println(events.getInt(1) + " : " + events.getString(2) + " : " + events.getString(3));
        assertTrue(events.next());
//        System.out.println(events.getInt(1) + " : " + events.getString(2) + " : " + events.getString(3));
        assertFalse(events.next());
    }
}
