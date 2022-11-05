/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static com.salesforce.apollo.state.Mutator.batch;
import static com.salesforce.apollo.state.Mutator.changeLog;
import static com.salesforce.apollo.state.Mutator.update;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.utils.Utils;

/**
 * @author hal.hildebrand
 *
 */
public class EmulationTest {

    @Test
    public void functional() throws Exception {
        // Resources to manage
        Executor exec = Executors.newSingleThreadExecutor(Utils.virtualThreadFactory());
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(Utils.virtualThreadFactory());

        // How long to wait until timing out ;)
        Duration timeout = Duration.ofSeconds(3);

        // The Emulator, using random values, in memory DB
        Emulator emmy = new Emulator();

        // Start the emulation. Here we're not providing any initial "genesis"
        // transactions for the chain
        emmy.start();

        // Mutator for all write (mutation) operations on the shard
        var mutator = emmy.getMutator();

        // Establish the book schema via a Liquibase migration transaction
        var results = mutator.execute(exec, update(changeLog(MigrationTest.BOOK_RESOURCE_PATH,
                                                             MigrationTest.BOOK_SCHEMA_ROOT)),
                                      timeout, scheduler);

        // Should have gotten something...
        assertNotNull(results);

        // Get the result of the migration, validate it was successful
        var set = results.get();
        assertNotNull(set);
        assertTrue(set.booleanValue());

        // Insert some rows into the DB
        var insertResults = mutator.execute(exec,
                                            batch("insert into test.books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)",
                                                  "insert into test.books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)",
                                                  "insert into test.books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)",
                                                  "insert into test.books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)",
                                                  "insert into test.books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)"),
                                            timeout, scheduler);
        assertNotNull(insertResults);
        var inserted = insertResults.get();
        assertNotNull(inserted);
        assertEquals(5, inserted.length);
        for (var i : inserted) {
            assertEquals(1, i);
        }

        // Get a read only JDBC connection
        var connection = emmy.newConnector();
        var statement = connection.createStatement();

        ResultSet books = statement.executeQuery("select * from test.books");
        assertTrue(books.first());
        for (int i = 0; i < 4; i++) {
            assertTrue(books.next(), "Missing row: " + (i + 1));
        }
    }
}
