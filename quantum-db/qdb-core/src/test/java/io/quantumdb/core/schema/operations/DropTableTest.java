package io.quantumdb.core.schema.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class DropTableTest {

    @Test
    public void testDropTable() {
        DropTable operation = SchemaOperations.dropTable("users");

        assertEquals("users", operation.getTableName());
    }

    @Test
    public void testDropTableThrowsExceptionWhenEmptyStringForTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.dropTable(""));
    }

    @Test
    public void testDropTableThrowsExceptionWhenNullForTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.dropTable(null));
    }

}
