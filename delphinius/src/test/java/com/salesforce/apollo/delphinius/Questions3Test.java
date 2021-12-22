/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import static com.salesforce.apollo.delphinius.schema.tables.Assertion.ASSERTION;
import static com.salesforce.apollo.delphinius.schema.tables.Edge.EDGE;
import static com.salesforce.apollo.delphinius.schema.tables.Relation.RELATION;
import static com.salesforce.apollo.delphinius.schema.tables.Subject.SUBJECT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.Random;

import org.h2.jdbc.JdbcConnection;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.Test;

import com.salesforce.apollo.delphinius.Oracle.Assertion;

import liquibase.Liquibase;
import liquibase.database.core.H2Database;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * @author hal.hildebrand
 *
 */
public class Questions3Test {

    @Test
    public void smokin() throws Exception {
        final var url = String.format("jdbc:h2:mem:test_engine-smoke-%s;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=3",
                                      new Random().nextLong());
        var connection = new JdbcConnection(url, new Properties(), "", "");

        var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
        try (Liquibase liquibase = new Liquibase("initialize.xml", new ClassLoaderResourceAccessor(), database)) {
            liquibase.update((String) null);
        }
        connection = new JdbcConnection(url, new Properties(), "", "");

        var ns = Oracle.namespace("MyOrg");
        var member = ns.relation("member");

        Oracle oracle = new Oracle(connection);
        oracle.map(ns.subject("HelpDesk", member), ns.subject("Admins", member));
        oracle.map(ns.subject("Ali"), ns.subject("Admins", member));
        oracle.map(ns.subject("Ali"), ns.subject("Users", member));
        oracle.map(ns.subject("Burcu"), ns.subject("Users", member));
        oracle.map(ns.subject("Can"), ns.subject("Users", member));
        oracle.map(ns.subject("Managers", member), ns.subject("Users", member));
        oracle.map(ns.subject("Technicians", member), ns.subject("Users", member));
        oracle.map(ns.subject("Demet"), ns.subject("HelpDesk", member));
        oracle.map(ns.subject("Egin"), ns.subject("HelpDesk", member));
        oracle.map(ns.subject("Egin"), ns.subject("Users", member));
        oracle.map(ns.subject("Fuat"), ns.subject("Managers", member));
        oracle.map(ns.subject("G l"), ns.subject("Managers", member));
        oracle.map(ns.subject("Hakan"), ns.subject("Technicians", member));
        oracle.map(ns.subject("Irmak"), ns.subject("Technicians", member));
        oracle.map(ns.subject("ABCTechnicians", member), ns.subject("Technicians", member));
        oracle.map(ns.subject("Jale"), ns.subject("ABCTechnicians", member));

        var dsl = DSL.using(connection);

        dumpEdges(dsl);

        var object = ns.object("Doc", ns.relation("View"));
        var subject = ns.subject("Users", member);
        Assertion tuple = subject.assertion(object);
        oracle.add(tuple);

        System.out.println("Tuples:\n" + dsl.selectFrom(ASSERTION).fetch());

        assertTrue(oracle.check(object.assertion(ns.subject("Jale"))));
        assertTrue(oracle.check(object.assertion(ns.subject("Egin"))));
        assertFalse(oracle.check(object.assertion(ns.subject("HelpDesk", member))));

        oracle.remove(ns.subject("ABCTechnicians", member), ns.subject("Technicians", member));

        assertFalse(oracle.check(object.assertion(ns.subject("Jale"))));
        assertTrue(oracle.check(object.assertion(ns.subject("Egin"))));
        assertFalse(oracle.check(object.assertion(ns.subject("HelpDesk", member))));

        dumpEdges(dsl);

        oracle.delete(tuple);

        assertFalse(oracle.check(object.assertion(ns.subject("Jale"))));
        assertFalse(oracle.check(object.assertion(ns.subject("Egin"))));
        assertFalse(oracle.check(object.assertion(ns.subject("HelpDesk", member))));
    }

    private void dumpEdges(DSLContext dsl) {
        var pa = SUBJECT.as("parent");
        var ch = SUBJECT.as("child");
        System.out.println(dsl.select(pa.NAME.as("parent"), pa.ID, ch.NAME.as("child"), ch.ID, EDGE.TRANSITIVE)
                              .from(pa, ch).join(EDGE).on(EDGE.PARENT.eq(pa.ID).and(EDGE.CHILD.eq(ch.ID)))
                              .orderBy(EDGE.PARENT, EDGE.CHILD, EDGE.TRANSITIVE).fetch());
    }
}
