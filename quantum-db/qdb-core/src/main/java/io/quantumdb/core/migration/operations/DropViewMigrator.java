// Generated by delombok at Thu Mar 11 18:53:07 PST 2021
package io.quantumdb.core.migration.operations;

import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.operations.DropView;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.Version;

public class DropViewMigrator implements SchemaOperationMigrator<DropView> {
	@Override
	public void migrate(Catalog catalog, RefLog refLog, Version version, DropView operation) {
		refLog.fork(version);
		refLog.dropView(version, operation.getViewName());
	}

	@java.lang.SuppressWarnings("all")
	DropViewMigrator() {
	}
}
