package io.quantumdb.core.schema.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class CopyTableTest {

    @Test
    public void testCopyTableOperation() {
        CopyTable operation = SchemaOperations.copyTable("users", "players");

        assertEquals("users", operation.getSourceTableName());
        assertEquals("players", operation.getTargetTableName());
    }

    @Test
    public void testThatCopyTableOperationThrowsExceptionWhenEmptyStringInputForSourceTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.copyTable("", "players"));
    }

    @Test
    public void testThatCopyTableOperationThrowsExceptionWhenEmptyStringInputForTargetTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.copyTable("users", ""));
    }

    @Test
    public void testThatCopyTableOperationThrowsExceptionWhenNullInputForSourceTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.copyTable(null, "players"));
    }

    @Test
    public void testThatCopyTableOperationThrowsExceptionWhenNullInputForTargetTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.copyTable("users", null));
    }

}
