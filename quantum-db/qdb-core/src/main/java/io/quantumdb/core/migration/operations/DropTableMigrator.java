
package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.DropTable;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;

public class DropTableMigrator implements SchemaOperationMigrator<DropTable> {
    
    DropTableMigrator() {
    }

    @Override
    public void migrate(Catalog catalog, RefLog refLog, Version version, DropTable operation) {
        refLog.fork(version);
        refLog.dropTable(version, operation.getTableName());
    }
}
