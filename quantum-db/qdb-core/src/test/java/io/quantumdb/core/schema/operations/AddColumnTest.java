package io.quantumdb.core.schema.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class AddColumnTest {

    @Test
    public void testAddingColumnWithDefaultExpression() {
        AddColumn operation = SchemaOperations.addColumn("users", "name", varchar(255), "'unknown'");

        assertEquals("users", operation.getTableName());
        assertEquals(new ColumnDefinition("name", varchar(255), "'unknown'"), operation.getColumnDefinition());
    }

    @Test
    public void testAddingColumnWithEmptyStringForColumnName() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.addColumn("users", "", varchar(255), NOT_NULL));
    }

    @Test
    public void testAddingColumnWithEmptyStringForTableName() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.addColumn("", "name", varchar(255), NOT_NULL));
    }

    @Test
    public void testAddingColumnWithNullForColumnName() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.addColumn("users", null, varchar(255), NOT_NULL));
    }

    @Test
    public void testAddingColumnWithNullForColumnType() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.addColumn("users", "name", null, NOT_NULL));
    }

    @Test
    public void testAddingColumnWithNullForTableName() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.addColumn(null, "name", varchar(255), NOT_NULL));
    }

    @Test
    public void testAddingColumnWithoutHints() {
        AddColumn operation = SchemaOperations.addColumn("users", "name", varchar(255), NOT_NULL);

        assertEquals("users", operation.getTableName());
        assertEquals(new ColumnDefinition("name", varchar(255), NOT_NULL), operation.getColumnDefinition());
    }

}
