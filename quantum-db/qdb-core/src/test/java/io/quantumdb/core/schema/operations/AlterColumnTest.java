package io.quantumdb.core.schema.operations;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.TestTypes;

public class AlterColumnTest {

    @Test
    public void testAddingHint() {
        AlterColumn operation = SchemaOperations.alterColumn("users", "name").addHint(Column.Hint.NOT_NULL);

        assertEquals(Sets.newHashSet(Column.Hint.NOT_NULL), operation.getHintsToAdd());
    }

    @Test
    public void testAddingHintThrowsExceptionWhenInputIsNull() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.alterColumn("users", "name").addHint(null));
    }

    @Test
    public void testDroppingDefaultExpression() {
        AlterColumn operation = SchemaOperations.alterColumn("users", "name").dropDefaultExpression();

        assertTrue(isNullOrEmpty(operation.getNewDefaultValueExpression().get()));
    }

    @Test
    public void testDroppingHint() {
        AlterColumn operation = SchemaOperations.alterColumn("users", "name").dropHint(Column.Hint.NOT_NULL);

        assertEquals(Sets.newHashSet(Column.Hint.NOT_NULL), operation.getHintsToDrop());
    }

    @Test
    public void testDroppingHintThrowsExceptionWhenInputIsNull() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.alterColumn("users", "name").dropHint(null));
    }

    @Test
    public void testModifyingDataType() {
        AlterColumn operation = SchemaOperations.alterColumn("users", "name").modifyDataType(TestTypes.varchar(255));

        assertEquals(TestTypes.varchar(255), operation.getNewColumnType().get());
    }

    public void testModifyingDataTypeThrowsExceptionWhenInputIsNull() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.alterColumn("users", "name").modifyDataType(null));
    }

    @Test
    public void testModifyingDefaultExpression() {
        AlterColumn operation = SchemaOperations.alterColumn("users", "name").modifyDefaultExpression("'unknown'");

        assertEquals("'unknown'", operation.getNewDefaultValueExpression().get());
    }

    @Test
    public void testModifyingDefaultExpressionThrowsExceptionWhenInputIsEmptyString() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.alterColumn("users", "name").modifyDefaultExpression(""));
    }

    @Test
    public void testModifyingDefaultExpressionThrowsExceptionWhenInputIsNull() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.alterColumn("users", "name").modifyDefaultExpression(null));
    }

    @Test
    public void testRenamingColumn() {
        AlterColumn operation = SchemaOperations.alterColumn("users", "name").rename("full_name");

        assertEquals("users", operation.getTableName());
        assertEquals("name", operation.getColumnName());
        assertEquals("full_name", operation.getNewColumnName().get());
    }

    public void testRenamingThrowsExceptionWhenInputIsEmptyString() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.alterColumn("users", "name").rename(""));
    }

    public void testRenamingThrowsExceptionWhenInputIsNull() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.alterColumn("users", "name").rename(null));
    }

}
