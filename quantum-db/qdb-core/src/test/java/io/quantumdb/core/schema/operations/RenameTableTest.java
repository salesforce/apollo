package io.quantumdb.core.schema.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class RenameTableTest {

    @Test
    public void testRenameTable() {
        RenameTable operation = SchemaOperations.renameTable("users", "players");

        assertEquals("users", operation.getTableName());
        assertEquals("players", operation.getNewTableName());
    }

    @Test
    public void testRenameTableThrowsExceptionWhenEmptyStringForNewTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.renameTable("users", ""));
    }

    @Test
    public void testRenameTableThrowsExceptionWhenEmptyStringForTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.renameTable("", "age"));
    }

    @Test
    public void testRenameTableThrowsExceptionWhenNullForNewTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.renameTable("users", null));
    }

    @Test
    public void testRenameTableThrowsExceptionWhenNullForTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.renameTable(null, "players"));
    }

}
