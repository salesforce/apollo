package io.quantumdb.core.schema.definitions;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.bigint;
import static io.quantumdb.core.schema.definitions.TestTypes.bool;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class TableTest {

    @Test
    public void testAddingColumnToTable() {
        new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));
    }

    @Test
    public void testAddingMutipleColumnsToTable() {
        new Table("users").addColumns(Lists.newArrayList(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT),
                                                         new Column("name", varchar(255), NOT_NULL)));
    }

    @Test
    public void testAddingNullColumnToTable() {
        assertThrows(IllegalArgumentException.class, () -> new Table("test-db").addColumn(null));
    }

    @Test
    public void testCreatingTable() {
        Table table = new Table("users");

        assertEquals("users", table.getName());
        assertTrue(table.getColumns().isEmpty());
    }

    @Test
    public void testCreatingTableWithEmptyStringForCatalogName() {
        assertThrows(IllegalArgumentException.class, () -> new Table(""));
    }

    @Test
    public void testCreatingTableWithNullForCatalogName() {
        assertThrows(IllegalArgumentException.class, () -> new Table(null));
    }

    @Test
    public void testRenamingTable() {
        Table table = new Table("users");
        table.rename("other_name");
    }

    @Test
    public void testThatAddColumnsMethodThrowsExceptionWhenInputIsNull() {
        assertThrows(IllegalArgumentException.class, () -> new Table("users").addColumns(null));
    }

    @Test
    public void testThatAddingColumnWithAnAlreadyTakenNameToTableThrowsException() {
        assertThrows(IllegalStateException.class,
                     () -> new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT))
                                             .addColumn(new Column("id", varchar(255))));
    }

    @Test
    public void testThatContainsColumnMethodReturnsFalseWhenColumnDoesNotExist() {
        Table table = new Table("users");

        assertFalse(table.containsColumn("id"));
    }

    @Test
    public void testThatContainsColumnMethodReturnsTrueWhenColumnExists() {
        Table table = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

        assertTrue(table.containsColumn("id"));
    }

    @Test
    public void testThatContainsColumnMethodThrowsExceptionOnEmptyStringInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Table table = new Table("users");
            table.containsColumn("");
        });
    }

    @Test
    public void testThatContainsColumnMethodThrowsExceptionOnNullInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Table table = new Table("users");
            table.containsColumn(null);
        });
    }

    @Test
    public void testThatCopyMethodReturnsCopy() {
        Column column = new Column("id", bigint(), IDENTITY, AUTO_INCREMENT);
        Table table = new Table("users").addColumn(column);

        Table copy = table.copy();

        assertEquals(table, copy);
        assertFalse(table == copy);
    }

    @Test
    public void testThatGetColumnMethodReturnsColumnWhenItExists() {
        Column column = new Column("id", bigint(), IDENTITY, AUTO_INCREMENT);
        Table table = new Table("users").addColumn(column);

        assertEquals(column, table.getColumn("id"));
    }

    @Test
    public void testThatGetColumnMethodThrowsExceptionOnEmptyStringInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Table table = new Table("users");
            table.getColumn("");
        });
    }

    @Test
    public void testThatGetColumnMethodThrowsExceptionOnNullInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Table table = new Table("users");
            table.getColumn(null);
        });
    }

    @Test
    public void testThatGetIdentityColumnMethodReturnsOneColumnForTableWithCompositeKey() {
        Column id1Column = new Column("left_id", bigint(), IDENTITY, AUTO_INCREMENT);
        Column id2Column = new Column("right_id", bigint(), IDENTITY, AUTO_INCREMENT);

        Table table = new Table("link_table").addColumn(id1Column)
                                             .addColumn(id2Column)
                                             .addColumn(new Column("some_property", bool(), NOT_NULL));

        assertEquals(Lists.newArrayList(id1Column, id2Column), table.getIdentityColumns());
    }

    @Test
    public void testThatGetIdentityColumnMethodReturnsOneColumnForTableWithoutIdentityColumns() {
        Table table = new Table("users").addColumn(new Column("name", varchar(255), NOT_NULL));

        assertTrue(table.getIdentityColumns().isEmpty());
    }

    @Test
    public void testThatGetIdentityColumnMethodReturnsOneColumnForTableWithSinglePrimaryKey() {
        Column idColumn = new Column("id", bigint(), IDENTITY, AUTO_INCREMENT);

        Table table = new Table("users").addColumn(idColumn).addColumn(new Column("name", varchar(255), NOT_NULL));

        assertEquals(Lists.newArrayList(idColumn), table.getIdentityColumns());
    }

    @Test
    public void testThatGetTableMethodThrowsExceptionWhenTableDoesNotExist() {
        assertThrows(IllegalStateException.class, () -> {
            Table table = new Table("users");
            table.getColumn("id");
        });
    }

    @Test
    public void testThatRemoveColumnMethodRemovesColumnWhenItExists() {
        Column column1 = new Column("id", bigint(), IDENTITY, AUTO_INCREMENT);
        Column column2 = new Column("name", varchar(255), NOT_NULL);
        Table table = new Table("users").addColumn(column1).addColumn(column2);

        Column removedColumn = table.removeColumn("name");
        assertEquals(column2, removedColumn);
        assertFalse(table.containsColumn("name"));
    }

    @Test
    public void testThatRemoveColumnMethodThrowsExceptionOnEmptyStringInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Table table = new Table("users");
            table.removeColumn("");
        });
    }

    @Test
    public void testThatRemoveColumnMethodThrowsExceptionOnNullInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Table table = new Table("users");
            table.removeColumn(null);
        });
    }

    @Test
    public void testThatRemoveColumnMethodThrowsExceptionWhenColumnDoesNotExist() {
        assertThrows(IllegalStateException.class, () -> {
            Table table = new Table("users");
            table.removeColumn("id");
        });
    }

    @Test
    public void testThatRenamingColumnIsReflectedInTable() {
        Column column = new Column("id", bigint(), IDENTITY, AUTO_INCREMENT);
        Table table = new Table("users").addColumn(column);

        column.rename("uuid");

        assertFalse(table.containsColumn("id"));
        assertTrue(table.containsColumn("uuid"));
        assertEquals(column, table.getColumn("uuid"));
    }

    @Test
    public void testThatRenamingColumnToEmptyStringThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            Column column = new Column("id", bigint(), IDENTITY, AUTO_INCREMENT);
            Table table = new Table("users").addColumn(column);
            table.rename("");
        });
    }

    @Test
    public void testThatRenamingTableToNullThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            Column column = new Column("id", bigint(), IDENTITY, AUTO_INCREMENT);
            Table table = new Table("users").addColumn(column);
            table.rename(null);
        });
    }

    @Test
    public void toStringReturnsSomething() {
        Table table = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

        assertFalse(Strings.isNullOrEmpty(table.toString()));
    }

}
