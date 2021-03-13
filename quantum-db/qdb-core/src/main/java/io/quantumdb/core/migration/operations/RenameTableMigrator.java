// Generated by delombok at Thu Mar 11 18:53:07 PST 2021
package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.RenameTable;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;

public class RenameTableMigrator implements SchemaOperationMigrator<RenameTable> {
	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, RenameTable operation) {
		refLog.fork(version);
		TableRef tableRef = refLog.getTableRef(version, operation.getTableName());
		TableRef ghost = tableRef.ghost(tableRef.getRefId(), version);
		ghost.rename(operation.getNewTableName());
	}

	@java.lang.SuppressWarnings("all")
	RenameTableMigrator() {
	}
}
