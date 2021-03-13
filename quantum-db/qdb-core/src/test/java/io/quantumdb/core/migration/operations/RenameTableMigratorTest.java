package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.integer;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;

public class RenameTableMigratorTest {

    private Catalog             catalog;
    private Changelog           changelog;
    private RenameTableMigrator migrator;
    private RefLog              refLog;

    @BeforeEach
    public void setUp() {
        this.catalog = new Catalog("test-db").addTable(new Table("users")
                                                                         .addColumn(new Column("id", integer(),
                                                                                 IDENTITY, NOT_NULL, AUTO_INCREMENT))
                                                                         .addColumn(new Column("name", varchar(255),
                                                                                 NOT_NULL)));

        this.changelog = new Changelog();
        this.refLog = RefLog.init(catalog, changelog.getRoot());

        this.migrator = new RenameTableMigrator();
    }

    @Test
    public void testExpandForRenamingTable() {
        RenameTable operation = SchemaOperations.renameTable("users", "customers");
        changelog.addChangeSet("Michael de Jong", "Renaming 'users' table to 'customers'.", operation);
        migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

        String refId = refLog.getTableRef(changelog.getLastAdded(), "customers").getRefId();
        Table ghostTable = catalog.getTable(refId);
        Table expectedGhostTable = new Table(refId)
                                                   .addColumn(new Column("id", integer(), IDENTITY, NOT_NULL,
                                                           AUTO_INCREMENT))
                                                   .addColumn(new Column("name", varchar(255), NOT_NULL));

        assertEquals(expectedGhostTable, ghostTable);
        assertEquals("users", refLog.getTableRef(changelog.getLastAdded(), "customers").getRefId());
        assertFalse(refLog.getTableRefs(changelog.getLastAdded())
                          .stream()
                          .anyMatch(tableRef -> tableRef.getName().equals("users")));
    }

}
