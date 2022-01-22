/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static com.salesforce.apollo.state.Mutator.batch;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.h2.jdbc.JdbcSQLSyntaxErrorException;
import org.joou.ULong;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Message;
import com.salesfoce.apollo.choam.proto.Transaction;
import com.salesfoce.apollo.state.proto.ChangeLog;
import com.salesfoce.apollo.state.proto.Migration;
import com.salesfoce.apollo.state.proto.Txn;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

/**
 * @author hal.hildebrand
 *
 */
public class MigrationTest {

    public static final Path   BOOK_RESOURCE_PATH = Path.of("src", "test", "resources", "book-schema");
    public static final String BOOK_SCHEMA_ROOT   = "bookSchema.xml";

    public static List<Message> initializeBookSchema() {
        return Collections.singletonList(Txn.newBuilder()
                                            .setMigration(MigrationTest.bookSchemaMigration())
                                            .setBatch(batch("create table books (id int, title varchar(50), author varchar(50), price float, qty int,  primary key (id))"))
                                            .build());
    }

    public static Migration bookSchemaMigration() {
        return Migration.newBuilder().setUpdate(Mutator.changeLog(BOOK_RESOURCE_PATH, BOOK_SCHEMA_ROOT)).build();
    }

    @Test
    public void rollback() throws Exception {
        SqlStateMachine updater = new SqlStateMachine("jdbc:h2:mem:test_migration-rollback", new Properties(),
                                                      new File("target/chkpoints"));
        final var executor = updater.getExecutor();
        executor.genesis(DigestAlgorithm.DEFAULT.getLast(), Collections.emptyList());

        Migration migration = Migration.newBuilder().setTag("test-1").build();
        CompletableFuture<Object> success = new CompletableFuture<>();
        executor.execute(0, Digest.NONE,
                         Transaction.newBuilder()
                                    .setContent(Txn.newBuilder().setMigration(migration).build().toByteString())
                                    .build(),
                         success);

        executor.beginBlock(ULong.valueOf(1), DigestAlgorithm.DEFAULT.getOrigin().prefix("voo"));

        migration = Migration.newBuilder().setUpdate(Mutator.changeLog(BOOK_RESOURCE_PATH, BOOK_SCHEMA_ROOT)).build();

        success = new CompletableFuture<>();
        executor.execute(0, Digest.NONE,
                         Transaction.newBuilder()
                                    .setContent(Txn.newBuilder().setMigration(migration).build().toByteString())
                                    .build(),
                         success);

        success.get(1, TimeUnit.SECONDS);

        Connection connection = updater.newConnection();
        Statement statement = connection.createStatement();
        ResultSet cb = statement.executeQuery("select * from test.books");

        assertFalse(cb.next(), "Should not exist");
        Transaction.Builder builder = Transaction.newBuilder();
        builder.setContent(Txn.newBuilder()
                              .setBatch(batch("insert into test.books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)",
                                              "insert into test.books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)",
                                              "insert into test.books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)",
                                              "insert into test.books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)",
                                              "insert into test.books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)"))
                              .build()
                              .toByteString());
        Transaction transaction = builder.build();

        updater.getExecutor().execute(0, Digest.NONE, transaction, null);

        ResultSet books = statement.executeQuery("select * from test.books");
        assertTrue(books.first());
        for (int i = 0; i < 4; i++) {
            assertTrue(books.next(), "Missing row: " + (i + 1));
        }

        migration = Migration.newBuilder()
                             .setRollback(ChangeLog.newBuilder()
                                                   .setRoot(BOOK_SCHEMA_ROOT)
                                                   .setResources(Mutator.resourcesFrom(BOOK_RESOURCE_PATH))
                                                   .setTag("test-1"))
                             .build();
        success = new CompletableFuture<>();

        executor.beginBlock(ULong.valueOf(2), DigestAlgorithm.DEFAULT.getOrigin().prefix("foo"));

        executor.execute(1, Digest.NONE,
                         Transaction.newBuilder()
                                    .setContent(Txn.newBuilder().setMigration(migration).build().toByteString())
                                    .build(),
                         success);

        success.get(1, TimeUnit.SECONDS);

        statement = connection.createStatement();
        try {
            cb = statement.executeQuery("select * from test.books");
            fail("Did not successfully roll back, test schema still exists");
        } catch (JdbcSQLSyntaxErrorException e) {
            // expected
        }
    }

    @Test
    public void update() throws Exception {
        SqlStateMachine updater = new SqlStateMachine("jdbc:h2:mem:test_migration-update", new Properties(),
                                                      new File("target/chkpoints"));
        final var executor = updater.getExecutor();
        executor.genesis(DigestAlgorithm.DEFAULT.getLast(), Collections.emptyList());

        Migration migration = Migration.newBuilder()
                                       .setUpdate(Mutator.changeLog(BOOK_RESOURCE_PATH, BOOK_SCHEMA_ROOT))
                                       .build();

        CompletableFuture<Object> success = new CompletableFuture<>();
        executor.execute(0, Digest.NONE,
                         Transaction.newBuilder()
                                    .setContent(Txn.newBuilder().setMigration(migration).build().toByteString())
                                    .build(),
                         success);

        success.get(1, TimeUnit.SECONDS);

        Connection connection = updater.newConnection();
        Statement statement = connection.createStatement();
        ResultSet cb = statement.executeQuery("select * from test.books");

        assertFalse(cb.next(), "Should not exist");
        Transaction.Builder builder = Transaction.newBuilder();
        builder.setContent(Txn.newBuilder()
                              .setBatch(batch("insert into test.books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)",
                                              "insert into test.books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)",
                                              "insert into test.books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)",
                                              "insert into test.books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)",
                                              "insert into test.books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)"))
                              .build()
                              .toByteString());
        Transaction transaction = builder.build();

        updater.getExecutor().execute(1, Digest.NONE, transaction, null);

        ResultSet books = statement.executeQuery("select * from test.books");
        assertTrue(books.first());
        for (int i = 0; i < 4; i++) {
            assertTrue(books.next(), "Missing row: " + (i + 1));
        }
    }
}
