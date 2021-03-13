package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.integer;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;

public class DropColumnMigratorTest {

    private Catalog            catalog;
    private Changelog          changelog;
    private DropColumnMigrator migrator;
    private RefLog             refLog;

    @BeforeEach
    public void setUp() {
        this.catalog = new Catalog("test-db").addTable(new Table("users")
                                                                         .addColumn(new Column("id", integer(),
                                                                                 IDENTITY, NOT_NULL, AUTO_INCREMENT))
                                                                         .addColumn(new Column("name", varchar(255),
                                                                                 NOT_NULL)));

        this.changelog = new Changelog();
        this.refLog = RefLog.init(catalog, changelog.getRoot());

        this.migrator = new DropColumnMigrator();
    }

    @Test
    public void testExpandForDroppingIdentityColumn() {
        assertThrows(IllegalStateException.class, () -> {
            DropColumn operation = SchemaOperations.dropColumn("users", "id");
            changelog.addChangeSet("Michael de Jong", "Dropped 'id' column from 'users' table.", operation);
            migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);
        });
    }

    @Test
    public void testExpandForDroppingSingleColumn() {
        DropColumn operation = SchemaOperations.dropColumn("users", "name");
        changelog.addChangeSet("Michael de Jong", "Dropped 'name' column from 'users' table.", operation);
        migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

        Table originalTable = catalog.getTable("users");
        Table ghostTable = getGhostTable(originalTable);

        Table expectedGhostTable = new Table(ghostTable.getName()).addColumn(new Column("id", integer(), IDENTITY,
                NOT_NULL, AUTO_INCREMENT));

        assertEquals(expectedGhostTable, ghostTable);
    }

    private Table getGhostTable(Table table) {
        TableRef tableRef = refLog.getTableRef(changelog.getLastAdded(), table.getName());
        String refId = tableRef.getRefId();
        return catalog.getTable(refId);
    }

}
