package io.quantumdb.core.schema.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;

@SuppressWarnings("deprecation")
public class DecomposeTableTest {

    @Test
    public void testDecomposingIntoTwoTables() {
        DecomposeTable operation = SchemaOperations.decomposeTable("users")
                                                   .into("names", "id", "name")
                                                   .into("addresses", "id", "address");

        ImmutableMultimap<String, String> expectedDecompositions = ImmutableMultimap.<String, String>builder()
                                                                                    .putAll("names",
                                                                                            Lists.newArrayList("id",
                                                                                                               "name"))
                                                                                    .putAll("addresses",
                                                                                            Lists.newArrayList("id",
                                                                                                               "address"))
                                                                                    .build();

        assertEquals("users", operation.getTableName());
        assertEquals(expectedDecompositions, operation.getDecompositions());
    }

    @Test
    public void testDecompositionWithEmptyStringForTableNameThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.decomposeTable(""));
    }

    @Test
    public void testDecompositionWithNullForTableNameThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.decomposeTable(null));
    }

    @Test
    public void testThatDecompositionWithEmptyStringForTargetTableNameThrowsException() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.decomposeTable("users").into("", "id", "name"));
    }

    @Test
    public void testThatDecompositionWithNullForTargetTableNameThrowsException() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.decomposeTable("users").into(null, "id", "name"));
    }

    @Test
    public void testThatDecompositionWithoutColumnsThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.decomposeTable("users").into("names"));
    }

}
