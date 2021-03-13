package io.quantumdb.core.schema.operations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Maps;

@SuppressWarnings("deprecation")
public class PartitionTableTest {

    @Test
    public void testPartitionTable() {
        PartitionTable operation = SchemaOperations.partitionTable("users")
                                                   .into("debtors", "credit < 0")
                                                   .into("creditors", "credit > 0");

        Map<String, String> partitions = Maps.newHashMap();
        partitions.put("debtors", "credit < 0");
        partitions.put("creditors", "credit > 0");

        assertEquals("users", operation.getTableName());
        assertEquals(partitions, operation.getPartitions());
    }

    @Test
    public void testThatDecompositionExpressionCannotBeEmptyString() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.partitionTable("users").into("creditors", ""));
    }

    @Test
    public void testThatDecompositionExpressionCannotBeNull() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.partitionTable("users").into("creditors", null));
    }

    @Test
    public void testThatDecompositionTableNameCannotBeEmptyString() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.partitionTable("users").into("", "credit < 0"));
    }

    @Test
    public void testThatDecompositionTableNameCannotBeNull() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.partitionTable("users").into(null, "credit < 0"));
    }

    @Test
    public void testThatTableNameCannotBeEmptyString() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.partitionTable(""));
    }

    @Test
    public void testThatTableNameCannotBeNull() {
        assertThrows(IllegalArgumentException.class, () -> SchemaOperations.partitionTable(null));
    }

    @Test
    public void testThatYouCannotDecomposeIntoTheSameTableTwice() {
        assertThrows(IllegalArgumentException.class,
                     () -> SchemaOperations.partitionTable("users")
                                           .into("creditors", "true")
                                           .into("creditors", "true"));
    }

}
