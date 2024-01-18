/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import com.salesforce.apollo.bloomFilters.BloomFilter;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.db.UniKERLDirectPooled;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.thoth.proto.Interval;
import com.salesforce.apollo.thoth.proto.Intervals;
import liquibase.Liquibase;
import liquibase.database.core.H2Database;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.h2.jdbcx.JdbcConnectionPool;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import java.security.SecureRandom;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author hal.hildebrand
 */
public class KerlSpaceTest {

    private static void initialize(JdbcConnectionPool connectionPoolA, JdbcConnectionPool connectionPoolB)
    throws SQLException {
        var database = new H2Database();
        try (var connection = connectionPoolA.getConnection()) {
            database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
            try (Liquibase liquibase = new Liquibase("/initialize-thoth.xml", new ClassLoaderResourceAccessor(),
                                                     database)) {
                liquibase.update((String) null);
            } catch (LiquibaseException e) {
                throw new IllegalStateException(e);
            }
        }

        database = new H2Database();
        try (var connection = connectionPoolB.getConnection()) {
            database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
            try (Liquibase liquibase = new Liquibase("/initialize-thoth.xml", new ClassLoaderResourceAccessor(),
                                                     database)) {
                liquibase.update((String) null);
            } catch (LiquibaseException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Test
    public void smokin() throws Exception {
        final var digestAlgorithm = DigestAlgorithm.DEFAULT;
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        JdbcConnectionPool connectionPoolA = JdbcConnectionPool.create("jdbc:h2:mem:A;DB_CLOSE_DELAY=-1", "", "");
        connectionPoolA.setMaxConnections(10);
        JdbcConnectionPool connectionPoolB = JdbcConnectionPool.create("jdbc:h2:mem:B;DB_CLOSE_DELAY=-1", "", "");
        connectionPoolB.setMaxConnections(10);

        var spaceA = new KerlSpace(connectionPoolA, DigestAlgorithm.DEFAULT.getOrigin());
        var stereotomyA = new StereotomyImpl(new MemKeyStore(),
                                             new UniKERLDirectPooled(connectionPoolA, digestAlgorithm).create(),
                                             entropy);
        var spaceB = new KerlSpace(connectionPoolB, DigestAlgorithm.DEFAULT.getLast());
        var stereotomyB = new StereotomyImpl(new MemKeyStore(),
                                             new UniKERLDirectPooled(connectionPoolB, digestAlgorithm).create(),
                                             entropy);

        initialize(connectionPoolA, connectionPoolB);

        var identifierA = stereotomyA.newIdentifier();
        try (var connection = connectionPoolA.getConnection()) {
            KerlDHT.updateLocationHash(identifierA.getIdentifier(), digestAlgorithm, DSL.using(connection));
        }

        identifierA.rotate();
        var digestA = identifierA.getLastEstablishingEvent().getCoordinates().getDigest();
        var biffA = spaceA.populate(0x1638, new CombinedIntervals(
        new KeyInterval(digestAlgorithm.getOrigin(), digestAlgorithm.getLast())), 0.000125);
        assertNotNull(biffA);
        var bffA = BloomFilter.from(biffA);

        var identifierB = stereotomyB.newIdentifier();
        identifierB.rotate();
        var digestB = identifierB.getLastEstablishingEvent().getCoordinates().getDigest();
        try (var connection = connectionPoolB.getConnection()) {
            KerlDHT.updateLocationHash(identifierB.getIdentifier(), digestAlgorithm, DSL.using(connection));
        }
        var biffB = spaceB.populate(0x1638, new CombinedIntervals(
        new KeyInterval(digestAlgorithm.getOrigin(), digestAlgorithm.getLast())), 0.000125);
        assertNotNull(biffB);
        var bffB = BloomFilter.from(biffB);

        assertTrue(bffA.contains(digestA));
        assertFalse(bffA.contains(digestB));

        assertTrue(bffB.contains(digestB));
        assertFalse(bffB.contains(digestA));

        assertNull(
        new UniKERLDirectPooled(connectionPoolA, digestAlgorithm).create().getKeyState(identifierB.getIdentifier()));
        assertNull(
        new UniKERLDirectPooled(connectionPoolB, digestAlgorithm).create().getKeyState(identifierA.getIdentifier()));

        var updateA = spaceA.reconcile(Intervals.newBuilder()
                                                .addIntervals(Interval.newBuilder()
                                                                      .setStart(digestAlgorithm.getOrigin().toDigeste())
                                                                      .setEnd(digestAlgorithm.getLast().toDigeste())
                                                                      .build())
                                                .setHave(biffB)
                                                .build(),
                                       new UniKERLDirectPooled(connectionPoolA, digestAlgorithm).create());
        assertNotNull(updateA);
        assertEquals(2, updateA.getEventsCount());

        var updateB = spaceB.reconcile(Intervals.newBuilder()
                                                .addIntervals(Interval.newBuilder()
                                                                      .setStart(digestAlgorithm.getOrigin().toDigeste())
                                                                      .setEnd(digestAlgorithm.getLast().toDigeste())
                                                                      .build())
                                                .setHave(biffA)
                                                .build(),
                                       new UniKERLDirectPooled(connectionPoolB, digestAlgorithm).create());
        assertNotNull(updateB);
        assertEquals(2, updateB.getEventsCount());

        spaceA.update(updateB.getEventsList(), new UniKERLDirectPooled(connectionPoolA, digestAlgorithm).create());
        spaceB.update(updateA.getEventsList(), new UniKERLDirectPooled(connectionPoolB, digestAlgorithm).create());

        assertNotNull(
        new UniKERLDirectPooled(connectionPoolA, digestAlgorithm).create().getKeyState(identifierB.getIdentifier()));
        assertNotNull(
        new UniKERLDirectPooled(connectionPoolB, digestAlgorithm).create().getKeyState(identifierA.getIdentifier()));
    }
}
