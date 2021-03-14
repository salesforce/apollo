
package io.quantumdb.core.migration.operations;

import java.util.Map;
import com.google.common.collect.ImmutableMap;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.AddColumn;
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.CopyTable;
import io.quantumdb.core.schema.operations.CreateIndex;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.CreateView;
import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.schema.operations.DropForeignKey;
import io.quantumdb.core.schema.operations.DropIndex;
import io.quantumdb.core.schema.operations.DropTable;
import io.quantumdb.core.schema.operations.DropView;
import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;

public class SchemaOperationsMigrator {
    private final Catalog                                                           catalog;
    private final Map<Class<? extends SchemaOperation>, SchemaOperationMigrator<?>> migrators;
    private final RefLog                                                            refLog;

    public SchemaOperationsMigrator(Catalog catalog, RefLog refLog) {
        this.catalog = catalog;
        this.refLog = refLog;
        this.migrators = ImmutableMap.<Class<? extends SchemaOperation>, SchemaOperationMigrator<?>>builder()
                                     .put(AddColumn.class, new AddColumnMigrator())
                                     .put(AddForeignKey.class, new AddForeignKeyMigrator())
                                     .put(AlterColumn.class, new AlterColumnMigrator())
                                     .put(CopyTable.class, new CopyTableMigrator())
                                     .put(CreateIndex.class, new CreateIndexMigrator())
                                     .put(CreateTable.class, new CreateTableMigrator())
                                     .put(DropIndex.class, new DropIndexMigrator())
                                     .put(DropTable.class, new DropTableMigrator())
                                     .put(DropColumn.class, new DropColumnMigrator())
                                     .put(DropForeignKey.class, new DropForeignKeyMigrator())
                                     .put(RenameTable.class, new RenameTableMigrator())
                                     .put(CreateView.class, new CreateViewMigrator())
                                     .put(DropView.class, new DropViewMigrator())
                                     .build();
    }

    
    public RefLog getRefLog() {
        return this.refLog;
    }

    public <T extends SchemaOperation> void migrate(Version version, T operation) {
        Class<?> type = operation.getClass();
        @SuppressWarnings("unchecked")
        SchemaOperationMigrator<T> migrator = (SchemaOperationMigrator<T>) migrators.get(type);
        if (migrator == null) {
            throw new UnsupportedOperationException("The operation: " + type + " is not (yet) supported!");
        }
        migrator.migrate(catalog, refLog, version, operation);
    }
}
