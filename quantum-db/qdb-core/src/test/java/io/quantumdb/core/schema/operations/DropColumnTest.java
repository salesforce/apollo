package io.quantumdb.core.schema.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class DropColumnTest {

    @Test
    public void testDropColumn() {
        DropColumn operation = SchemaOperations.dropColumn("users", "age");

        assertEquals("users", operation.getTableName());
        assertEquals("age", operation.getColumnName());
    }

    @Test
    public void testDropColumnThrowsExceptionWhenEmptyStringForColumnName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.dropColumn("users", ""));
    }

    @Test
    public void testDropColumnThrowsExceptionWhenEmptyStringForTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.dropColumn("", "age"));
    }

    @Test
    public void testDropColumnThrowsExceptionWhenNullForColumnName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.dropColumn("users", null));
    }

    @Test
    public void testDropColumnThrowsExceptionWhenNullForTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.dropColumn(null, "age"));
    }

}
