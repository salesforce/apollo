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

        // Namespace
        var ns = Oracle.namespace("my-org");

        // relations
        var member = ns.relation("member");
        var flag = ns.relation("flag");

        // Group membersip
        var userMembers = ns.subject("Users", member);
        var adminMembers = ns.subject("Admins", member);
        var helpDeskMembers = ns.subject("HelpDesk", member);
        var managerMembers = ns.subject("Managers", member);
        var technicianMembers = ns.subject("Technicians", member);
        var abcTechMembers = ns.subject("ABCTechnicians", member);
        var flaggedTechnicianMembers = ns.subject(abcTechMembers.name(), flag);

        // Flagged subjects for testing
        var egin = ns.subject("Egin", flag);
        var ali = ns.subject("Ali", flag);
        var gl = ns.subject("G l", flag);
        var fuat = ns.subject("Fuat", flag);

        // Subjects
        var jale = ns.subject("Jale");
        var irmak = ns.subject("Irmak");
        var hakan = ns.subject("Hakan");
        var demet = ns.subject("Demet");
        var can = ns.subject("Can");
        var burcu = ns.subject("Burcu");

        oracle.map(helpDeskMembers, adminMembers);
        oracle.map(ali, adminMembers);
        oracle.map(ali, userMembers);
        oracle.map(burcu, userMembers);
        oracle.map(can, userMembers);
        oracle.map(managerMembers, userMembers);
        oracle.map(technicianMembers, userMembers);
        oracle.map(demet, helpDeskMembers);
        oracle.map(egin, helpDeskMembers);
        oracle.map(egin, userMembers);
        oracle.map(fuat, managerMembers);
        oracle.map(gl, managerMembers);
        oracle.map(hakan, technicianMembers);
        oracle.map(irmak, technicianMembers);
        oracle.map(abcTechMembers, technicianMembers);
        oracle.map(flaggedTechnicianMembers, technicianMembers);
        oracle.map(jale, abcTechMembers);

        var dsl = DSL.using(connection);

        dumpEdges(dsl);

        // Protected resource namespace
        var docNs = Oracle.namespace("Document");
        // Permission
        var view = docNs.relation("View");
        // Protected Object
        var object123View = docNs.object("123", view);

        Assertion tuple = userMembers.assertion(object123View);
        oracle.add(tuple);

        var viewers = oracle.read(object123View);
        assertEquals(1, viewers.size());
        assertTrue(viewers.contains(userMembers), "Should contain: " + userMembers);

        Assertion grantTechs = flaggedTechnicianMembers.assertion(object123View);
        oracle.add(grantTechs);

        System.out.println(dsl.selectFrom(ASSERTION).fetch());

        viewers = oracle.read(object123View);
        assertEquals(2, viewers.size());
        assertTrue(viewers.contains(userMembers), "Should contain: " + userMembers);
        assertTrue(viewers.contains(flaggedTechnicianMembers), "Should contain: " + flaggedTechnicianMembers);

        var flaggedViewers = oracle.read(flag, object123View);
        assertEquals(1, flaggedViewers.size());
        assertTrue(flaggedViewers.contains(flaggedTechnicianMembers), "Should contain: " + flaggedTechnicianMembers);

        var inferredViewers = oracle.expand(object123View);
        assertEquals(14, inferredViewers.size());
        for (var s : Arrays.asList(ali, jale, egin, irmak, hakan, gl, fuat, can, burcu, managerMembers,
                                   technicianMembers, abcTechMembers, userMembers, flaggedTechnicianMembers)) {
            assertTrue(inferredViewers.contains(s), "Should contain: " + s);
        }

        var inferredFlaggedViewers = oracle.expand(flag, object123View);
        assertEquals(5, inferredFlaggedViewers.size());
        for (var s : Arrays.asList(egin, ali, gl, fuat, flaggedTechnicianMembers)) {
            assertTrue(inferredFlaggedViewers.contains(s), "Should contain: " + s);
        }

        assertTrue(oracle.check(object123View.assertion(jale)));
        assertTrue(oracle.check(object123View.assertion(egin)));
        assertFalse(oracle.check(object123View.assertion(helpDeskMembers)));

        oracle.remove(abcTechMembers, technicianMembers);

        assertFalse(oracle.check(object123View.assertion(jale)));
        assertTrue(oracle.check(object123View.assertion(egin)));
        assertFalse(oracle.check(object123View.assertion(helpDeskMembers)));

        dumpEdges(dsl);

        oracle.delete(tuple);

        assertFalse(oracle.check(object123View.assertion(jale)));
        assertFalse(oracle.check(object123View.assertion(egin)));
        assertFalse(oracle.check(object123View.assertion(helpDeskMembers)));
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
