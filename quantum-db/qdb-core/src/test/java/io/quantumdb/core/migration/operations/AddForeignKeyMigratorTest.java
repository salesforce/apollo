package io.quantumdb.core.migration.operations;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.TestTypes.integer;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;

public class AddForeignKeyMigratorTest {

    private Catalog               catalog;
    private Changelog             changelog;
    private AddForeignKeyMigrator migrator;
    private RefLog                refLog;

    @BeforeEach
    public void setUp() {
        this.catalog = new Catalog("test-db")
                                             .addTable(new Table("users")
                                                                         .addColumn(new Column("id", integer(),
                                                                                 IDENTITY, NOT_NULL, AUTO_INCREMENT))
                                                                         .addColumn(new Column("name", varchar(255),
                                                                                 NOT_NULL)))
                                             .addTable(new Table("posts")
                                                                         .addColumn(new Column("id", integer(),
                                                                                 IDENTITY, NOT_NULL, AUTO_INCREMENT))
                                                                         .addColumn(new Column("author", integer(),
                                                                                 NOT_NULL)));

        this.changelog = new Changelog();
        this.refLog = RefLog.init(catalog, changelog.getRoot());
        this.migrator = new AddForeignKeyMigrator();
    }

    @Test
    public void testExpandForAddingSingleColumn() {
        AddForeignKey operation = SchemaOperations.addForeignKey("posts", "author").referencing("users", "id");
        changelog.addChangeSet("Michael de Jong", "Added 'date_of_birth' column to 'users' table.", operation);
        migrator.migrate(catalog, refLog, changelog.getLastAdded(), operation);

        Table usersTable = catalog.getTable("users");
        Table originalTable = catalog.getTable("posts");
        Table ghostTable = getGhostTable(originalTable);

        Table expectedGhostTable = new Table(ghostTable.getName())
                                                                  .addColumn(new Column("id", integer(), IDENTITY,
                                                                          NOT_NULL, AUTO_INCREMENT))
                                                                  .addColumn(new Column("author", integer(), NOT_NULL));

        expectedGhostTable.addForeignKey("author").referencing(usersTable, "id");

        assertEquals(expectedGhostTable, ghostTable);
    }

    private Table getGhostTable(Table table) {
        TableRef tableRef = refLog.getTableRef(changelog.getLastAdded(), table.getName());
        return catalog.getTable(tableRef.getRefId());
    }

}
