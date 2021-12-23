/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import static com.salesforce.apollo.delphinius.schema.tables.Assertion.ASSERTION;
import static com.salesforce.apollo.delphinius.schema.tables.Edge.EDGE;
import static com.salesforce.apollo.delphinius.schema.tables.Subject.SUBJECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.h2.jdbc.JdbcConnection;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

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
        Oracle oracle = new Oracle(connection);

        var ns = Oracle.namespace("MyOrg");
        var member = ns.relation("member");
        var flag = ns.relation("flag");
        var view = ns.relation("View");

        var users = ns.subject("Users", member);
        var admins = ns.subject("Admins", member);
        var helpDesk = ns.subject("HelpDesk", member);
        var managers = ns.subject("Managers", member);
        var technicians = ns.subject("Technicians", member);
        var abcTech = ns.subject("ABCTechnicians", member);

        var egin = ns.subject("Egin", flag);
        var ali = ns.subject("Ali", flag);
        var gl = ns.subject("G l", flag);
        var fuat = ns.subject("Fuat", flag);

        var jale = ns.subject("Jale");
        var irmak = ns.subject("Irmak");
        var hakan = ns.subject("Hakan");
        var demet = ns.subject("Demet");
        var can = ns.subject("Can");
        var burcu = ns.subject("Burcu");

        var object = ns.object("Doc", view);

        oracle.map(helpDesk, admins);
        oracle.map(ali, admins);
        oracle.map(ali, users);
        oracle.map(burcu, users);
        oracle.map(can, users);
        oracle.map(managers, users);
        oracle.map(technicians, users);
        oracle.map(demet, helpDesk);
        oracle.map(egin, helpDesk);
        oracle.map(egin, users);
        oracle.map(fuat, managers);
        oracle.map(gl, managers);
        oracle.map(hakan, technicians);
        oracle.map(irmak, technicians);
        oracle.map(abcTech, technicians);
        oracle.map(jale, abcTech);

        var dsl = DSL.using(connection);

        dumpEdges(dsl);

        Assertion tuple = users.assertion(object);
        oracle.add(tuple);

        var viewers = oracle.read(object);
        assertEquals(13, viewers.size());
        for (var s : Arrays.asList(ali, jale, egin, irmak, hakan, gl, fuat, can, burcu, managers, technicians, abcTech,
                                   users)) {
            assertTrue(viewers.contains(s), "Should contain: " + s);
        }

        var flaggedViewers = oracle.read(flag, object);
        assertEquals(4, flaggedViewers.size());
        for (var s : Arrays.asList(egin, ali, gl, fuat)) {
            assertTrue(flaggedViewers.contains(s), "Should contain: " + s);
        }

        System.out.println("Assertions:\n" + dsl.selectFrom(ASSERTION).fetch());

        assertTrue(oracle.check(object.assertion(jale)));
        assertTrue(oracle.check(object.assertion(egin)));
        assertFalse(oracle.check(object.assertion(helpDesk)));

        oracle.remove(abcTech, technicians);

        assertFalse(oracle.check(object.assertion(jale)));
        assertTrue(oracle.check(object.assertion(egin)));
        assertFalse(oracle.check(object.assertion(helpDesk)));

        dumpEdges(dsl);

        oracle.delete(tuple);

        assertFalse(oracle.check(object.assertion(jale)));
        assertFalse(oracle.check(object.assertion(egin)));
        assertFalse(oracle.check(object.assertion(helpDesk)));
    }

    private void dumpEdges(DSLContext dsl) {
        var pa = SUBJECT.as("parent");
        var ch = SUBJECT.as("child");
        System.out.println(dsl.select(pa.NAME.as("parent"), pa.ID, ch.NAME.as("child"), ch.ID, EDGE.TRANSITIVE)
                              .from(pa, ch)
                              .join(EDGE)
                              .on(EDGE.PARENT.eq(pa.ID).and(EDGE.CHILD.eq(ch.ID)))
                              .orderBy(EDGE.PARENT, EDGE.CHILD, EDGE.TRANSITIVE)
                              .fetch());
    }
}
