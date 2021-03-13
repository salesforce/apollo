package io.quantumdb.core.schema.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.bigint;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

public class CreateTableTest {

    @Test
    public void testCreatingTableWithDefaultExpression() {
        CreateTable operation = SchemaOperations.createTable("addresses")
                                                .with("id", bigint(), "'1'", IDENTITY, NOT_NULL);

        List<ColumnDefinition> expectedColumns = Lists.newArrayList(new ColumnDefinition("id", bigint(), "'1'",
                IDENTITY, NOT_NULL));

        assertEquals("addresses", operation.getTableName());
        assertEquals(expectedColumns, operation.getColumns());
    }

    @Test
    public void testCreatingTableWithEmptyStringForColumnName() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.createTable("addresses")
                                           .with("", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL));
    }

    @Test
    public void testCreatingTableWithEmptyStringForTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.createTable(""));
    }

    @Test
    public void testCreatingTableWithMultipleColumns() {
        CreateTable operation = SchemaOperations.createTable("addresses")
                                                .with("id", bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL)
                                                .with("street", varchar(255), NOT_NULL)
                                                .with("street_number", varchar(10), NOT_NULL)
                                                .with("city", varchar(255), NOT_NULL)
                                                .with("postal_code", varchar(10), NOT_NULL)
                                                .with("country", varchar(255), NOT_NULL);

        List<ColumnDefinition> expectedColumns = Lists.newArrayList(new ColumnDefinition("id", bigint(), IDENTITY,
                AUTO_INCREMENT, NOT_NULL), new ColumnDefinition("street", varchar(255), NOT_NULL),
                                                                    new ColumnDefinition("street_number", varchar(10),
                                                                            NOT_NULL),
                                                                    new ColumnDefinition("city", varchar(255),
                                                                            NOT_NULL),
                                                                    new ColumnDefinition("postal_code", varchar(10),
                                                                            NOT_NULL),
                                                                    new ColumnDefinition("country", varchar(255),
                                                                            NOT_NULL));

        assertEquals("addresses", operation.getTableName());
        assertEquals(expectedColumns, operation.getColumns());
    }

    @Test
    public void testCreatingTableWithNullForColumnName() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.createTable("addresses")
                                           .with(null, bigint(), IDENTITY, AUTO_INCREMENT, NOT_NULL));
    }

    @Test
    public void testCreatingTableWithNullForColumnType() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.createTable("addresses")
                                           .with("id", null, IDENTITY, AUTO_INCREMENT, NOT_NULL));
    }

    @Test
    public void testCreatingTableWithNullForTableName() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.createTable(null));
    }

}
