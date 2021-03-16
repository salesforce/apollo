package io.quantumdb.core.backends.integration.multistate;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bigint;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bool;
import static io.quantumdb.core.schema.definitions.PostgresTypes.varchar;
import static io.quantumdb.core.schema.operations.SchemaOperations.addColumn;
import static io.quantumdb.core.schema.operations.SchemaOperations.createTable;
import static io.quantumdb.core.schema.operations.SchemaOperations.execute;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.backends.PostgresqlDatabase;
import io.quantumdb.core.migration.Migrator;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;

@Disabled
public class MultiStateTest extends PostgresqlDatabase {

    private Backend backend;
    private Version step0;
    private Version step1;
    private Version step4;

    @Override
    @AfterEach
    public void after() throws Exception {
        super.after();
    }

    @Override
    @BeforeEach
    public void before() throws Exception {
        super.before();

        Config config = new Config();
        config.setUrl(getJdbcUrl());
        config.setUser(getJdbcUser());
        config.setPassword(getJdbcPass());
        config.setCatalog(getCatalogName());
        config.setDriver(getJdbcDriver());

        backend = config.getBackend();

        State state = backend.loadState();
        Changelog changelog = state.getChangelog();

        step0 = changelog.getRoot();

        step1 = changelog.addChangeSet("step1", "Michael de Jong", "Create test table.",
                                       createTable("test").with("id", bigint(), IDENTITY, AUTO_INCREMENT))
                         .getLastAdded();

        changelog.addChangeSet("step2", "Michael de Jong", "Add name column to test table.",
                               addColumn("test", "name", varchar(255), "''", NOT_NULL))
                 .getLastAdded();

        changelog.addChangeSet("step3", "Michael de Jong", "Insert default user account into test table.",
                               execute("INSERT INTO test (name) VALUES ('Hello');"))
                 .getLastAdded();

        step4 = changelog.addChangeSet("step4", "Michael de Jong", "Created admin flag for test table.",
                                       addColumn("test", "admin", bool(), "'false'", NOT_NULL))
                         .getLastAdded();

        backend.persistState(state);
    }

    @Test
    public void testMigratingOverDataChange() throws MigrationException {
        Migrator migrator = new Migrator(backend);
        migrator.migrate(step0.getId(), step1.getId());
        migrator.migrate(step1.getId(), step4.getId());
        migrator.drop(step1.getId());
    }

}
