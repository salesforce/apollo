package io.quantumdb.core.backends.integration.videostores;

import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.CUSTOMERS_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.FILMS_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.INVENTORY_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.PAYCHECKS_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.PAYMENTS_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.RENTALS_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.STAFF_ID;
import static io.quantumdb.core.backends.integration.videostores.PostgresqlBaseScenario.STORES_ID;
import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.PostgresTypes.date;
import static io.quantumdb.core.schema.definitions.PostgresTypes.floats;
import static io.quantumdb.core.schema.definitions.PostgresTypes.integer;
import static io.quantumdb.core.schema.definitions.PostgresTypes.varchar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import io.quantumdb.core.backends.DatabaseMigrator.MigrationException;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.SchemaOperations;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;

@Disabled
public class CreateCreditCardsTable {

    public static PostgresqlBaseScenario setup = new PostgresqlBaseScenario();

    private static Version origin;
    private static State   state;
    private static Version target;

    @AfterAll
    public static void after() throws Exception {
        setup.after();
    }

    @BeforeAll
    public static void before() throws Exception {
        setup.before();
    }

    @BeforeAll
    public static void performEvolution() throws SQLException, MigrationException {
        setup.insertTestData();

        origin = setup.getChangelog().getLastAdded();

        setup.getChangelog()
             .addChangeSet("test", "Michael de Jong",
                           SchemaOperations.createTable("credit_cards")
                                           .with("credit_card_number", varchar(255), IDENTITY, NOT_NULL)
                                           .with("card_holder_name", varchar(255), NOT_NULL)
                                           .with("expiration_date", date(), NOT_NULL));

        target = setup.getChangelog().getLastAdded();
        setup.getBackend().persistState(setup.getState());

        setup.getMigrator().migrate(origin.getId(), target.getId());

        state = setup.getBackend().loadState();
    }

    @Test
    public void verifyTableMappings() {
        RefLog refLog = state.getRefLog();

        // Unchanged tables
        assertEquals(FILMS_ID, refLog.getTableRef(target, "films").getRefId());
        assertEquals(CUSTOMERS_ID, refLog.getTableRef(target, "customers").getRefId());
        assertEquals(PAYMENTS_ID, refLog.getTableRef(target, "payments").getRefId());
        assertEquals(RENTALS_ID, refLog.getTableRef(target, "rentals").getRefId());
        assertEquals(STORES_ID, refLog.getTableRef(target, "stores").getRefId());
        assertEquals(STAFF_ID, refLog.getTableRef(target, "staff").getRefId());
        assertEquals(INVENTORY_ID, refLog.getTableRef(target, "inventory").getRefId());
        assertEquals(PAYCHECKS_ID, refLog.getTableRef(target, "paychecks").getRefId());

        // New tables
        assertNotNull(refLog.getTableRef(target, "credit_cards").getRefId());
    }

    @Test
    public void verifyTableStructure() {
        RefLog refLog = state.getRefLog();

        // Original tables and foreign keys.

        Table stores = new Table(refLog.getTableRef(origin, "stores").getRefId()).addColumn(new Column("id", integer(),
                IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                 .addColumn(new Column("name",
                                                                                         varchar(255), NOT_NULL))
                                                                                 .addColumn(new Column("manager_id",
                                                                                         integer(), NOT_NULL));

        Table staff = new Table(refLog.getTableRef(origin, "staff").getRefId()).addColumn(new Column("id", integer(),
                IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                               .addColumn(new Column("name",
                                                                                       varchar(255), NOT_NULL))
                                                                               .addColumn(new Column("store_id",
                                                                                       integer(), NOT_NULL));

        Table customers = new Table(refLog.getTableRef(origin, "customers").getRefId()).addColumn(new Column("id",
                integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                       .addColumn(new Column("name",
                                                                                               varchar(255), NOT_NULL))
                                                                                       .addColumn(new Column("store_id",
                                                                                               integer(), NOT_NULL))
                                                                                       .addColumn(new Column(
                                                                                               "referred_by",
                                                                                               integer()));

        Table films = new Table(refLog.getTableRef(origin, "films").getRefId()).addColumn(new Column("id", integer(),
                IDENTITY, AUTO_INCREMENT, NOT_NULL)).addColumn(new Column("name", varchar(255), NOT_NULL));

        Table inventory = new Table(refLog.getTableRef(origin, "inventory").getRefId()).addColumn(new Column("id",
                integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                       .addColumn(new Column("store_id",
                                                                                               integer(), NOT_NULL))
                                                                                       .addColumn(new Column("film_id",
                                                                                               integer(), NOT_NULL));

        Table paychecks = new Table(refLog.getTableRef(origin, "paychecks").getRefId()).addColumn(new Column("id",
                integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                       .addColumn(new Column("staff_id",
                                                                                               integer(), NOT_NULL))
                                                                                       .addColumn(new Column("date",
                                                                                               date(), NOT_NULL))
                                                                                       .addColumn(new Column("amount",
                                                                                               floats(), NOT_NULL));

        Table payments = new Table(refLog.getTableRef(origin, "payments").getRefId())
                                                                                     .addColumn(new Column("id",
                                                                                             integer(), IDENTITY,
                                                                                             AUTO_INCREMENT, NOT_NULL))
                                                                                     .addColumn(new Column("staff_id",
                                                                                             integer()))
                                                                                     .addColumn(new Column(
                                                                                             "customer_id", integer(),
                                                                                             NOT_NULL))
                                                                                     .addColumn(new Column("rental_id",
                                                                                             integer(), NOT_NULL))
                                                                                     .addColumn(new Column("date",
                                                                                             date(), NOT_NULL))
                                                                                     .addColumn(new Column("amount",
                                                                                             floats(), NOT_NULL));

        Table rentals = new Table(refLog.getTableRef(origin, "rentals").getRefId())
                                                                                   .addColumn(new Column("id",
                                                                                           integer(), IDENTITY,
                                                                                           AUTO_INCREMENT, NOT_NULL))
                                                                                   .addColumn(new Column("staff_id",
                                                                                           integer()))
                                                                                   .addColumn(new Column("customer_id",
                                                                                           integer(), NOT_NULL))
                                                                                   .addColumn(new Column("inventory_id",
                                                                                           integer(), NOT_NULL))
                                                                                   .addColumn(new Column("date", date(),
                                                                                           NOT_NULL));

        stores.addForeignKey("manager_id").referencing(staff, "id");
        staff.addForeignKey("store_id").referencing(stores, "id");
        customers.addForeignKey("referred_by").referencing(customers, "id");
        customers.addForeignKey("store_id").referencing(stores, "id");
        inventory.addForeignKey("store_id").referencing(stores, "id");
        inventory.addForeignKey("film_id").referencing(films, "id");
        paychecks.addForeignKey("staff_id").referencing(staff, "id");
        payments.addForeignKey("staff_id").referencing(staff, "id");
        payments.addForeignKey("customer_id").referencing(customers, "id");
        payments.addForeignKey("rental_id").referencing(rentals, "id");
        rentals.addForeignKey("staff_id").referencing(staff, "id");
        rentals.addForeignKey("customer_id").referencing(customers, "id");
        rentals.addForeignKey("inventory_id").referencing(inventory, "id");

        // New tables and foreign keys.

        Table creditCards = new Table(refLog.getTableRef(target, "credit_cards").getRefId()).addColumn(new Column(
                "credit_card_number", varchar(255), IDENTITY, NOT_NULL))
                                                                                            .addColumn(new Column(
                                                                                                    "card_holder_name",
                                                                                                    varchar(255),
                                                                                                    NOT_NULL))
                                                                                            .addColumn(new Column(
                                                                                                    "expiration_date",
                                                                                                    date(), NOT_NULL));

        List<Table> tables = Lists.newArrayList(stores, staff, customers, films, inventory, paychecks, payments,
                                                rentals, creditCards);

        Catalog expected = new Catalog(setup.getCatalog().getName());
        tables.forEach(expected::addTable);

        assertEquals(expected.getTables(), state.getCatalog().getTables());
    }

}
