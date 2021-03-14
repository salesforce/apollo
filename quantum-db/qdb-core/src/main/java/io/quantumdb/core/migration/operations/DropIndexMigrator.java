
package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropIndex;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;

class DropIndexMigrator implements SchemaOperationMigrator<DropIndex> {
    
    DropIndexMigrator() {
    }

    @Override
    public void migrate(Catalog catalog, RefLog refLog, Version version, DropIndex operation) {
        String tableName = operation.getTableName();
        TransitiveTableMirrorer.mirror(catalog, refLog, version, tableName);
        TableRef tableRef = refLog.getTableRef(version, tableName);
        String refId = tableRef.getRefId();
        Table table = catalog.getTable(refId);
        table.removeIndex(operation.getColumns());
    }
}
