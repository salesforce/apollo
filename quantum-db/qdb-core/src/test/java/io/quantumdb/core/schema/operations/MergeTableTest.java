package io.quantumdb.core.schema.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

@SuppressWarnings("deprecation")
public class MergeTableTest {

    @Test
    public void testMergeTable() {
        MergeTable operation = SchemaOperations.mergeTable("users", "users_old", "users_new");

        assertEquals("users", operation.getLeftTableName());
        assertEquals("users_old", operation.getRightTableName());
        assertEquals("users_new", operation.getTargetTableName());
    }

    @Test
    public void testMergeTableThrowsExceptionWhenEmptyStringForLeftTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.mergeTable("", "users_old", "users_new"));
    }

    @Test
    public void testMergeTableThrowsExceptionWhenEmptyStringForRightTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.mergeTable("users", "", "users_new"));
    }

    @Test
    public void testMergeTableThrowsExceptionWhenEmptyStringForTargetTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.mergeTable("users", "users_old", ""));
    }

    @Test
    public void testMergeTableThrowsExceptionWhenNullForLeftTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.mergeTable(null, "users_old", "users_new"));
    }

    @Test
    public void testMergeTableThrowsExceptionWhenNullForRightTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.mergeTable("users", null, "users_new"));
    }

    @Test
    public void testMergeTableThrowsExceptionWhenNullForTargetTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.mergeTable("users", "users_old", null));
    }

}
