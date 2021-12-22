/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class ScriptTest {

    @Test
    public void smoke() throws Exception {
        SqlStateMachine machine = new SqlStateMachine("jdbc:h2:mem:test_script", new Properties(),
                                                      new File("target/chkpoints"));
        machine.getExecutor().genesis(0, DigestAlgorithm.DEFAULT.getLast(), Collections.emptyList());
        Connection connection = machine.newConnection();
        createAndInsert(connection);
        connection.commit();
        Txn txn = Txn.newBuilder()
                     .setScript(new Mutator(null,
                                            machine.getSession()).callScript("test.DbAccess", "call",
                                                                             Utils.getDocument(getClass().getResourceAsStream("/scripts/dbaccess.java"))))
                     .build();
        CompletableFuture<Object> completion = new CompletableFuture<>();
        machine.getExecutor().execute(0, Digest.NONE, Transaction.newBuilder().setContent(txn.toByteString()).build(),
                                      completion);

        assertTrue(ResultSet.class.isAssignableFrom(completion.get().getClass()));
        ResultSet rs = (ResultSet) completion.get();
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
    }

    private Statement createAndInsert(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        Statement s = connection.createStatement();

        s.execute("create schema s");
        s.execute("create table s.books (id int, title varchar(50), author varchar(50), price float, qty int)");

        s.execute("insert into s.books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)");
        s.execute("insert into s.books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)");
        s.execute("insert into s.books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)");
        s.execute("insert into s.books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)");
        s.execute("insert into s.books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");
        return s;
    }
}
