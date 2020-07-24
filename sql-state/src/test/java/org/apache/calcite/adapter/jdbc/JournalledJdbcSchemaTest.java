/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.adapter.jdbc.programs.ForcedRulesProgram;
import org.apache.calcite.adapter.jdbc.programs.SequenceProgram;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Holder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class JournalledJdbcSchemaTest {
    private static final String DBURL = "jdbc:h2:mem:db1";

    private Map<String, Object> options;

    @BeforeAll
    public static void configureInMemoryDB() throws Exception {
        Class.forName(org.h2.Driver.class.getName());
        Connection connection = DriverManager.getConnection(DBURL);
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE MY_TABLE_J (id INT NOT NULL, myvfield TIMESTAMP, mysvfield TIMESTAMP)");
        statement.execute("CREATE TABLE OTHER_TABLE (id INT NOT NULL, myvfield TIMESTAMP, mysvfield TIMESTAMP)");
        statement.close();
        connection.close();
    }

    @BeforeEach
    public void prepare() {
        JournalledJdbcSchema.Factory.INSTANCE.setAutomaticallyAddRules(false);

        options = new HashMap<>();
        options.put("jdbcDriver", org.h2.Driver.class.getName());
        options.put("jdbcUrl", DBURL);
        options.put("journalVersionField", "myvfield");
        options.put("journalSubsequentVersionField", "mysvfield");
        options.put("journalSuffix", "_J");
        options.put("journalDefaultKey", ImmutableList.of("def1", "def2"));
        options.put("journalTables", ImmutableMap.of("MY_TABLE", ImmutableList.of("myKey1", "myKey2")));
    }

    @Test
    public void testFactoryCreatesJournalledJdbcSchema() {
        Schema schema = JournalledJdbcSchema.Factory.INSTANCE.create(null, "my-parent", options);
        assertTrue(schema instanceof JournalledJdbcSchema);

        JournalledJdbcSchema journalledSchema = (JournalledJdbcSchema) schema;

        assertEquals(journalledSchema.getVersionField(), "myvfield");
        assertEquals(journalledSchema.getSubsequentVersionField(), "mysvfield");
    }

    @Test
    public void testFactoryWillAutomaticallyAddRules() {
        // This test changes the global state of Calcite! It shouldn't cause issues
        // elsewhere since the rules avoid
        // changing unrecognised tables, so will not apply to their own output.
        JournalledJdbcSchema.Factory.INSTANCE.setAutomaticallyAddRules(true);
        JournalledJdbcSchema.Factory.INSTANCE.create(null, "my-parent", options);
        try {
            Program def = Mockito.mock(Program.class);
            Holder<Program> holder = Holder.of(def);
            Hook.PROGRAM.run(holder);
            assertTrue(holder.get() instanceof SequenceProgram);
            assertTrue(((SequenceProgram) holder.get()).getPrograms().get(0) instanceof ForcedRulesProgram);
        } finally {
            // ensure no gap where another hook may be added
            JournalledJdbcSchema.Factory.INSTANCE.setAutomaticallyAddRules(false);
            JournalledJdbcRuleManager.removeHook();
        }
    }

    private JournalledJdbcSchema makeSchema() {
        return (JournalledJdbcSchema) JournalledJdbcSchema.Factory.INSTANCE.create(null, "my-parent", options);
    }

    @Test
    public void testCanLoadConnectionDetailsFromExternalFile() {
        options.remove("jdbcDriver");
        options.remove("jdbcUrl");
        options.put("connection", "connection.json");
        makeSchema();
    }

    public void testFactoryRejectsAbsentJournalTables() {
        try {
            options.remove("journalTables");
            makeSchema();
            fail("expected runtime exception");
        } catch (RuntimeException e) {
            // expected
        }
    }

    public void testFactoryRejectsInvalidConnections() {
        try {
            options.put("connection", "missingFile.json");
            makeSchema();
            fail("expected runtime exception");
        } catch (RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testDefaultVersionField() {
        options.remove("journalVersionField");
        JournalledJdbcSchema schema = makeSchema();
        assertEquals(schema.getVersionField(), "version_number");
    }

    @Test
    public void testDefaultSubsequentVersionField() {
        options.remove("journalSubsequentVersionField");
        JournalledJdbcSchema schema = makeSchema();
        assertEquals(schema.getSubsequentVersionField(), "subsequent_version_number");
    }

    @Test
    public void testDefaultJournalSuffix() {
        options.remove("journalSuffix");
        JournalledJdbcSchema schema = makeSchema();
        assertEquals(schema.journalNameFor("foo"), "foo_journal");
    }

    @Test
    public void testDefaultsToTimestampVersioning() {
        JournalledJdbcSchema schema = makeSchema();
        assertEquals(schema.getVersionType(), JournalVersionType.TIMESTAMP);
    }

    @Test
    public void testVersionTypeCanBeChanged() {
        options.put("journalVersionType", "BIGINT");
        JournalledJdbcSchema schema = makeSchema();
        assertEquals(schema.getVersionType(), JournalVersionType.BIGINT);
    }

    @Test
    public void testVersionTypeIsCaseInsensitive() {
        options.put("journalVersionType", "bigint");
        JournalledJdbcSchema schema = makeSchema();
        assertEquals(schema.getVersionType(), JournalVersionType.BIGINT);
    }

    public void testVersionTypeMustBeKnown() {
        try {
            options.put("journalVersionType", "nope");
            makeSchema();
            fail("expected runtime exception");
        } catch (RuntimeException e) {
            // expected
        }
    }

    @Test
    public void testGetTableNamesReturnsVirtualTables() {
        JournalledJdbcSchema schema = makeSchema();
        Set<String> names = schema.getTableNames();
        assertTrue(names.contains("MY_TABLE"));
        assertTrue(names.contains("OTHER_TABLE"));
    }

    @Test
    public void testGetTableConvertsMatchingTables() {
        JournalledJdbcSchema schema = makeSchema();
        Table table = schema.getTable("MY_TABLE");
        assertNotNull(table);
        assertTrue(table instanceof JournalledJdbcTable);
        JournalledJdbcTable journalTable = (JournalledJdbcTable) table;
        assertEquals(journalTable.getVersionField(), "myvfield");
        assertEquals(journalTable.getSubsequentVersionField(), "mysvfield");
        assertNotNull(journalTable.getJournalTable());
    }

    @Test
    public void testGetTablePassesThroughNonMatchingTables() {
        JournalledJdbcSchema schema = makeSchema();
        Table table = schema.getTable("OTHER_TABLE");
        assertNotNull(table);
        assertFalse(table instanceof JournalledJdbcTable);
    }

    @Test
    public void testGetTableSurvivesNulls() {
        JournalledJdbcSchema schema = makeSchema();
        Table table = schema.getTable("NOT_HERE");
        assertNull(table);
    }

    @Test
    public void testListsOfKeysAreLoaded() {
        JournalledJdbcSchema schema = makeSchema();
        JournalledJdbcTable journalTable = (JournalledJdbcTable) schema.getTable("MY_TABLE");
        assertEquals(journalTable.getKeyColumnNames(), ImmutableList.of("myKey1", "myKey2"));
    }

    @Test
    public void testSingleStringKeysAreLoaded() {
        options.put("journalTables", ImmutableMap.of("MY_TABLE", "foo"));
        JournalledJdbcSchema schema = makeSchema();
        JournalledJdbcTable journalTable = (JournalledJdbcTable) schema.getTable("MY_TABLE");
        assertEquals(journalTable.getKeyColumnNames(), ImmutableList.of("foo"));
    }

    @Test
    public void testDefaultKeysAreLoadedIfNoSpecificKeysGiven() {
        options.put("journalTables", Collections.singletonMap("MY_TABLE", null));
        JournalledJdbcSchema schema = makeSchema();
        JournalledJdbcTable journalTable = (JournalledJdbcTable) schema.getTable("MY_TABLE");
        assertEquals(journalTable.getKeyColumnNames(), ImmutableList.of("def1", "def2"));
    }

    @Test
    public void testListsOfTablesAreGivenDefaultKeys() {
        options.put("journalTables", ImmutableList.of("MY_TABLE"));
        JournalledJdbcSchema schema = makeSchema();
        JournalledJdbcTable journalTable = (JournalledJdbcTable) schema.getTable("MY_TABLE");
        assertEquals(journalTable.getKeyColumnNames(), ImmutableList.of("def1", "def2"));
    }

    public void testMissingKeysAreRejected() {
        try {
            options.remove("journalDefaultKey");
            options.put("journalTables", Collections.singletonMap("MY_TABLE", null));
            makeSchema();
            fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
