package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.bool;
import static io.quantumdb.core.schema.definitions.TestTypes.date;
import static io.quantumdb.core.schema.definitions.TestTypes.integer;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;

public class AddColumnMigratorTest {

    private Catalog           catalog;
    private Changelog         changelog;
    private AddColumnMigrator migrator;
    private RefLog            refLog;

    @BeforeEach
    public void setUp() {
        this.catalog = new Catalog("test-db").addTable(new Table("users")
                                                                         .addColumn(new Column("id", integer(),
                                                                                 IDENTITY, NOT_NULL, AUTO_INCREMENT))
                                                                         .addColumn(new Column("name", varchar(255),
                                                                                 NOT_NULL)));

        this.changelog = new Changelog();
        this.refLog = RefLog.init(catalog, changelog.getRoot());

        this.migrator = new AddColumnMigrator();
    }

    @Test
    public void testExpandForAddingMultipleColumns() {
        AddColumn operation1 = SchemaOperations.addColumn("users", "date_of_birth", date(), "NULL");
        changelog.addChangeSet("Michael de Jong", "Added 'date_of_birth' column to 'users' table.", operation1);
        migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation1);

        AddColumn operation2 = SchemaOperations.addColumn("users", "activated", bool(), "TRUE");
        changelog.addChangeSet("Michael de Jong", "Added 'activated' column to 'users' table.", operation2);
        migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation2);

        Table originalTable = catalog.getTable("users");
        Table ghostTable = getGhostTable(originalTable);

        Table expectedGhostTable = new Table(ghostTable.getName())
                                                                  .addColumn(new Column("id", integer(), IDENTITY,
                                                                          NOT_NULL, AUTO_INCREMENT))
                                                                  .addColumn(new Column("name", varchar(255), NOT_NULL))
                                                                  .addColumn(new Column("date_of_birth", date(),
                                                                          "NULL"))
                                                                  .addColumn(new Column("activated", bool(), "TRUE"));

        assertEquals(3, catalog.getTables().size());
        assertEquals(expectedGhostTable, ghostTable);
    }

    @Test
    public void testExpandForAddingSingleColumn() {
        AddColumn operation = SchemaOperations.addColumn("users", "date_of_birth", date(), "NULL");
        changelog.addChangeSet("Michael de Jong", "Added 'date_of_birth' column to 'users' table.", operation);
        migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

        Table originalTable = catalog.getTable("users");
        Table ghostTable = getGhostTable(originalTable);

        Table expectedGhostTable = new Table(ghostTable.getName())
                                                                  .addColumn(new Column("id", integer(), IDENTITY,
                                                                          NOT_NULL, AUTO_INCREMENT))
                                                                  .addColumn(new Column("name", varchar(255), NOT_NULL))
                                                                  .addColumn(new Column("date_of_birth", date(),
                                                                          "NULL"));

        assertEquals(expectedGhostTable, ghostTable);
    }

    private Table getGhostTable(Table table) {
        TableRef tableRef = refLog.getTableRef(changelog.getLastAdded(), table.getName());
        String refId = tableRef.getRefId();
        return catalog.getTable(refId);
    }

}
