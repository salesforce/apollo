package io.quantumdb.core.schema.definitions;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.bigint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.google.common.base.Strings;

public class CatalogTest {

    @Test
    public void testAddingNullTableToCatalog() {
        assertThrows(IllegalArgumentException.class, () -> new Catalog("test-db").addTable(null));
    }

    @Test
    public void testAddingTableToCatalog() {
        Table table = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

        Catalog catalog = new Catalog("test-db").addTable(table);

        assertEquals(catalog, table.getParent());
    }

    @Test
    public void testCreatingCatalog() {
        Catalog catalog = new Catalog("test-db");

        assertEquals("test-db", catalog.getName());
        assertTrue(catalog.getTables().isEmpty());
    }

    @Test
    public void testCreatingCatalogWithEmptyStringForCatalogName() {
        assertThrows(IllegalArgumentException.class, () -> new Catalog(""));
    }

    @Test
    public void testCreatingCatalogWithNullForCatalogName() {
        assertThrows(IllegalArgumentException.class, () -> new Catalog(null));
    }

    @Test
    public void testRemovingTableDropsOutgoingForeignKeys() {
        Catalog catalog = new Catalog("test-db");
        Table users = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
                                        .addColumn(new Column("address_id", bigint(), NOT_NULL));

        Table addresses = new Table("addresses").addColumn(new Column("id", bigint(), IDENTITY, NOT_NULL,
                AUTO_INCREMENT));

        catalog.addTable(users);
        catalog.addTable(addresses);

        users.addForeignKey("address_id").referencing(addresses, "id");

        catalog.removeTable("users");

        assertTrue(addresses.getColumn("id").getIncomingForeignKeys().isEmpty());
        assertTrue(addresses.getForeignKeys().isEmpty());
    }

    @Test
    public void testRemovingTableThrowsExceptionWhenIncomingForeignKeysExist() {
        assertThrows(IllegalStateException.class, () -> {
            Catalog catalog = new Catalog("test-db");
            Table users = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, NOT_NULL, AUTO_INCREMENT))
                                            .addColumn(new Column("address_id", bigint(), NOT_NULL));

            Table addresses = new Table("addresses").addColumn(new Column("id", bigint(), IDENTITY, NOT_NULL,
                    AUTO_INCREMENT));

            catalog.addTable(users);
            catalog.addTable(addresses);

            users.addForeignKey("address_id").referencing(addresses, "id");

            catalog.removeTable("addresses");
        });
    }

    @Test
    public void testThatContainsTableMethodReturnsFalseWhenTableDoesNotExist() {
        Catalog catalog = new Catalog("test-db");

        assertFalse(catalog.containsTable("users"));
    }

    @Test
    public void testThatContainsTableMethodReturnsTrueWhenTableExists() {
        Table table = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

        Catalog catalog = new Catalog("test-db").addTable(table);

        assertTrue(catalog.containsTable("users"));
    }

    @Test
    public void testThatContainsTableMethodThrowsExceptionOnEmptyStringInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Catalog catalog = new Catalog("test-db");
            catalog.containsTable("");
        });
    }

    @Test
    public void testThatContainsTableMethodThrowsExceptionOnNullInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Catalog catalog = new Catalog("test-db");
            catalog.containsTable(null);
        });
    }

    @Test
    public void testThatCopyMethodReturnsCopy() {
        Table table = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

        Catalog catalog = new Catalog("test-db").addTable(table);

        Catalog copy = catalog.copy();

        assertEquals(catalog, copy);
        assertFalse(catalog == copy);
    }

    @Test
    public void testThatGetTableMethodReturnsTableWhenItExists() {
        Table table = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

        Catalog catalog = new Catalog("test-db").addTable(table);

        assertEquals(table, catalog.getTable("users"));
    }

    @Test
    public void testThatGetTableMethodThrowsExceptionOnEmptyStringInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Catalog catalog = new Catalog("test-db");
            catalog.getTable("");
        });
    }

    @Test
    public void testThatGetTableMethodThrowsExceptionOnNullInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Catalog catalog = new Catalog("test-db");
            catalog.getTable(null);
        });
    }

    @Test
    public void testThatGetTableMethodThrowsExceptionWhenTableDoesNotExist() {
        assertThrows(IllegalStateException.class, () -> {
            Catalog catalog = new Catalog("test-db");
            catalog.getTable("users");
        });
    }

    @Test
    public void testThatRemoveTableMethodRemovesTableWhenItExists() {
        Table table = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

        Catalog catalog = new Catalog("test-db").addTable(table);

        Table removedTable = catalog.removeTable("users");
        assertEquals(table, removedTable);
        assertFalse(catalog.containsTable("users"));
    }

    @Test
    public void testThatRemoveTableMethodThrowsExceptionOnEmptyStringInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Catalog catalog = new Catalog("test-db");
            catalog.removeTable("");
        });
    }

    @Test
    public void testThatRemoveTableMethodThrowsExceptionOnNullInput() {
        assertThrows(IllegalArgumentException.class, () -> {
            Catalog catalog = new Catalog("test-db");
            catalog.removeTable(null);
        });
    }

    @Test
    public void testThatRemoveTableMethodThrowsExceptionWhenTableDoesNotExist() {
        assertThrows(IllegalStateException.class, () -> {
            Catalog catalog = new Catalog("test-db");
            catalog.removeTable("users");
        });
    }

    @Test
    public void testThatRenamingTableIsReflectedInCatalog() {
        Table table = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

        Catalog catalog = new Catalog("test-db").addTable(table);

        table.rename("players");

        assertFalse(catalog.containsTable("users"));
        assertTrue(catalog.containsTable("players"));
        assertEquals(table, catalog.getTable("players"));
    }

    @Test
    public void testThatRenamingTableThrowsExceptionWhenNameIsAlreadyTaken() {
        assertThrows(IllegalStateException.class, () -> {
            Table usersTable = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

            Table playersTable = new Table("players").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

            new Catalog("test-db").addTable(usersTable).addTable(playersTable);

            usersTable.rename("players");
        });
    }

    @Test
    public void toStringReturnsSomething() {
        Table table = new Table("users").addColumn(new Column("id", bigint(), IDENTITY, AUTO_INCREMENT));

        Catalog catalog = new Catalog("test-db").addTable(table);

        assertFalse(Strings.isNullOrEmpty(catalog.toString()));
    }

}
