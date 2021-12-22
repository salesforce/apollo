/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.delphinius;

import static com.salesforce.apollo.delphinius.schema.tables.Edge.EDGE;
import static com.salesforce.apollo.delphinius.schema.tables.Subject.SUBJECT;
import static com.salesforce.apollo.delphinius.schema.tables.Tuple.TUPLE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.Random;

import org.h2.jdbc.JdbcConnection;
import org.jooq.impl.DSL;
import org.junit.Test;

import com.salesforce.apollo.delphinius.Oracle.Tuple;
import com.salesforce.apollo.delphinius.schema.tables.Subject;

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

        var foo = Oracle.namespace("foo");
        Oracle oracle = new Oracle(connection);
        oracle.map(foo.subject("HelpDesk"), foo.subject("Admins"));
        oracle.map(foo.subject("Ali"), foo.subject("Admins"));
        oracle.map(foo.subject("Ali"), foo.subject("Users"));
        oracle.map(foo.subject("Burcu"), foo.subject("Users"));
        oracle.map(foo.subject("Can"), foo.subject("Users"));
        oracle.map(foo.subject("Managers"), foo.subject("Users"));
        oracle.map(foo.subject("Technicians"), foo.subject("Users"));
        oracle.map(foo.subject("Demet"), foo.subject("HelpDesk"));
        oracle.map(foo.subject("Egin"), foo.subject("HelpDesk"));
        oracle.map(foo.subject("Egin"), foo.subject("Users"));
        oracle.map(foo.subject("Fuat"), foo.subject("Managers"));
        oracle.map(foo.subject("G l"), foo.subject("Managers"));
        oracle.map(foo.subject("Hakan"), foo.subject("Technicians"));
        oracle.map(foo.subject("Irmak"), foo.subject("Technicians"));
        oracle.map(foo.subject("ABCTechnicians"), foo.subject("Technicians"));
        oracle.map(foo.subject("Jale"), foo.subject("ABCTechnicians"));

        var dsl = DSL.using(connection);

        Subject pa = SUBJECT.as("parent");
        Subject ch = SUBJECT.as("child");
        System.out.println(dsl.select(pa.NAME.as("parent"), pa.ID, ch.NAME.as("child"), ch.ID, EDGE.HOPS).from(pa, ch)
                              .join(EDGE).on(EDGE.PARENT.eq(pa.ID).and(EDGE.CHILD.eq(ch.ID)))
                              .orderBy(EDGE.PARENT, EDGE.CHILD, EDGE.HOPS).fetch());

        var relation = foo.relation("View");
        var object = foo.object("Doc", foo.relation("Viewer"));
        var subject = foo.subject("Users");
        Tuple tuple = object.tuple(relation, subject);
        oracle.add(tuple);

        System.out.println("Tuples:\n" + dsl.selectFrom(TUPLE).fetch());

        assertTrue(oracle.check(object.tuple(relation, foo.subject("Jale"))));
        assertTrue(oracle.check(object.tuple(relation, foo.subject("Egin"))));
        assertFalse(oracle.check(object.tuple(relation, foo.subject("HelpDesk"))));

        oracle.remove(foo.subject("ABCTechnicians"), foo.subject("Technicians"));

        assertFalse(oracle.check(object.tuple(relation, foo.subject("Jale"))));
        assertTrue(oracle.check(object.tuple(relation, foo.subject("Egin"))));
        assertFalse(oracle.check(object.tuple(relation, foo.subject("HelpDesk"))));

        System.out.println(dsl.select(pa.NAME.as("parent"), pa.ID, ch.NAME.as("child"), ch.ID, EDGE.HOPS).from(pa, ch)
                              .join(EDGE).on(EDGE.PARENT.eq(pa.ID).and(EDGE.CHILD.eq(ch.ID)))
                              .orderBy(EDGE.PARENT, EDGE.CHILD, EDGE.HOPS).fetch());

        oracle.delete(tuple);

        assertFalse(oracle.check(object.tuple(relation, foo.subject("Jale"))));
        assertFalse(oracle.check(object.tuple(relation, foo.subject("Egin"))));
        assertFalse(oracle.check(object.tuple(relation, foo.subject("HelpDesk"))));
    }
}
