
package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.AddForeignKey;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;

class AddForeignKeyMigrator implements SchemaOperationMigrator<AddForeignKey> {
    
    AddForeignKeyMigrator() {
    }

    @Override
    public void migrate(Catalog catalog, RefLog refLog, Version version, AddForeignKey operation) {
        String tableName = operation.getReferringTableName();
        TransitiveTableMirrorer.mirror(catalog, refLog, version, tableName);
        TableRef tableRef = refLog.getTableRef(version, tableName);
        Table table = catalog.getTable(tableRef.getRefId());
        String referencedTableName = operation.getReferencedTableName();
        TableRef referencedTableRef = refLog.getTableRef(version, referencedTableName);
        Table referencedTable = catalog.getTable(referencedTableRef.getRefId());
        table.addForeignKey(operation.getReferringColumnNames())
             .named(operation.getName())
             .onDelete(operation.getOnDelete())
             .onUpdate(operation.getOnUpdate())
             .referencing(referencedTable, operation.getReferencedColumnNames());
    }
}
