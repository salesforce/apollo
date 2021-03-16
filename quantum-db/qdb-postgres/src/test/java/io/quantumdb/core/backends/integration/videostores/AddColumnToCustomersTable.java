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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

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
public class AddColumnToCustomersTable {

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
             .addChangeSet("test", "Michael de Jong", SchemaOperations.addColumn("customers", "date_of_birth", date()));

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

        // Ghosted tables
        assertNotEquals(STORES_ID, refLog.getTableRef(target, "stores").getRefId());
        assertNotEquals(STAFF_ID, refLog.getTableRef(target, "staff").getRefId());
        assertNotEquals(INVENTORY_ID, refLog.getTableRef(target, "inventory").getRefId());
        assertNotEquals(PAYCHECKS_ID, refLog.getTableRef(target, "paychecks").getRefId());
        assertNotEquals(CUSTOMERS_ID, refLog.getTableRef(target, "customers").getRefId());
        assertNotEquals(PAYMENTS_ID, refLog.getTableRef(target, "payments").getRefId());
        assertNotEquals(RENTALS_ID, refLog.getTableRef(target, "rentals").getRefId());
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

        Table newStores = new Table(refLog.getTableRef(target, "stores").getRefId()).addColumn(new Column("id",
                integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                    .addColumn(new Column("name",
                                                                                            varchar(255), NOT_NULL))
                                                                                    .addColumn(new Column("manager_id",
                                                                                            integer(), NOT_NULL));

        Table newStaff = new Table(refLog.getTableRef(target, "staff").getRefId()).addColumn(new Column("id", integer(),
                IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                  .addColumn(new Column("name",
                                                                                          varchar(255), NOT_NULL))
                                                                                  .addColumn(new Column("store_id",
                                                                                          integer(), NOT_NULL));

        Table newInventory = new Table(refLog.getTableRef(target, "inventory").getRefId()).addColumn(new Column("id",
                integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                          .addColumn(new Column(
                                                                                                  "store_id", integer(),
                                                                                                  NOT_NULL))
                                                                                          .addColumn(new Column(
                                                                                                  "film_id", integer(),
                                                                                                  NOT_NULL));

        Table newPaychecks = new Table(refLog.getTableRef(target, "paychecks").getRefId()).addColumn(new Column("id",
                integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                          .addColumn(new Column(
                                                                                                  "staff_id", integer(),
                                                                                                  NOT_NULL))
                                                                                          .addColumn(new Column("date",
                                                                                                  date(), NOT_NULL))
                                                                                          .addColumn(new Column(
                                                                                                  "amount", floats(),
                                                                                                  NOT_NULL));

        Table newCustomers = new Table(refLog.getTableRef(target, "customers").getRefId()).addColumn(new Column("id",
                integer(), customers.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                          .addColumn(new Column("name",
                                                                                                  varchar(255),
                                                                                                  NOT_NULL))
                                                                                          .addColumn(new Column(
                                                                                                  "store_id", integer(),
                                                                                                  NOT_NULL))
                                                                                          .addColumn(new Column(
                                                                                                  "referred_by",
                                                                                                  integer()))
                                                                                          .addColumn(new Column(
                                                                                                  "date_of_birth",
                                                                                                  date()));

        Table newPayments = new Table(refLog.getTableRef(target, "payments").getRefId()).addColumn(new Column("id",
                integer(), payments.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                        .addColumn(new Column(
                                                                                                "staff_id", integer()))
                                                                                        .addColumn(new Column(
                                                                                                "customer_id",
                                                                                                integer(), NOT_NULL))
                                                                                        .addColumn(new Column(
                                                                                                "rental_id", integer(),
                                                                                                NOT_NULL))
                                                                                        .addColumn(new Column("date",
                                                                                                date(), NOT_NULL))
                                                                                        .addColumn(new Column("amount",
                                                                                                floats(), NOT_NULL));

        Table newRentals = new Table(refLog.getTableRef(target, "rentals").getRefId()).addColumn(new Column("id",
                integer(), rentals.getColumn("id").getSequence(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
                                                                                      .addColumn(new Column("staff_id",
                                                                                              integer()))
                                                                                      .addColumn(new Column(
                                                                                              "customer_id", integer(),
                                                                                              NOT_NULL))
                                                                                      .addColumn(new Column(
                                                                                              "inventory_id", integer(),
                                                                                              NOT_NULL))
                                                                                      .addColumn(new Column("date",
                                                                                              date(), NOT_NULL));

        newStores.addForeignKey("manager_id").referencing(newStaff, "id");
        newStaff.addForeignKey("store_id").referencing(newStores, "id");
        newInventory.addForeignKey("store_id").referencing(newStores, "id");
        newInventory.addForeignKey("film_id").referencing(films, "id");
        newPaychecks.addForeignKey("staff_id").referencing(newStaff, "id");
        newCustomers.addForeignKey("referred_by").referencing(newCustomers, "id");
        newCustomers.addForeignKey("store_id").referencing(newStores, "id");
        newPayments.addForeignKey("staff_id").referencing(newStaff, "id");
        newPayments.addForeignKey("customer_id").referencing(newCustomers, "id");
        newPayments.addForeignKey("rental_id").referencing(newRentals, "id");
        newRentals.addForeignKey("staff_id").referencing(newStaff, "id");
        newRentals.addForeignKey("customer_id").referencing(newCustomers, "id");
        newRentals.addForeignKey("inventory_id").referencing(inventory, "id");

        List<Table> tables = Lists.newArrayList(stores, staff, customers, films, inventory, paychecks, payments,
                                                rentals, newCustomers, newPayments, newRentals, newStores, newStaff,
                                                newInventory, newPaychecks);

        Catalog expected = new Catalog(setup.getCatalog().getName());
        tables.forEach(expected::addTable);

        assertEquals(expected.getTables(), state.getCatalog().getTables());
    }

}
