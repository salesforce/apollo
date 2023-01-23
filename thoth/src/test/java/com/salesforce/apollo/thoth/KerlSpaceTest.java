/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;

import org.h2.jdbcx.JdbcConnectionPool;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

import com.salesfoce.apollo.thoth.proto.Interval;
import com.salesfoce.apollo.thoth.proto.Intervals;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.db.UniKERLDirectPooled;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;

import liquibase.Liquibase;
import liquibase.database.core.H2Database;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * @author hal.hildebrand
 *
 */
public class KerlSpaceTest {

    @Test
    public void smokin() throws Exception {
        final var digestAlgorithm = DigestAlgorithm.DEFAULT;
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        JdbcConnectionPool connectionPoolA = JdbcConnectionPool.create("jdbc:h2:mem:A;DB_CLOSE_DELAY=-1", "", "");
        connectionPoolA.setMaxConnections(10);
        var spaceA = new KerlSpace(connectionPoolA);
        var kerlPoolA = new UniKERLDirectPooled(connectionPoolA, digestAlgorithm);
        final var kerlA = kerlPoolA.create();
        var stereotomyA = new StereotomyImpl(new MemKeyStore(), kerlA, entropy);
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

        JdbcConnectionPool connectionPoolB = JdbcConnectionPool.create("jdbc:h2:mem:B;DB_CLOSE_DELAY=-1", "", "");
        connectionPoolB.setMaxConnections(10);
        var spaceB = new KerlSpace(connectionPoolB);
        var kerlPoolB = new UniKERLDirectPooled(connectionPoolB, digestAlgorithm);
        final var kerlB = kerlPoolB.create();
        var stereotomyB = new StereotomyImpl(new MemKeyStore(), kerlB, entropy);
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

        var identifierA = stereotomyA.newIdentifier().get();
        try (var connection = connectionPoolA.getConnection()) {
            KerlDHT.updateLocationHash(identifierA.getIdentifier(), digestAlgorithm, DSL.using(connection));
        }
        identifierA.rotate().get();
        var digestA = identifierA.getLastEstablishingEvent().get().getCoordinates().getDigest();
        var biffA = spaceA.populate(0x1638, new CombinedIntervals(new KeyInterval(digestAlgorithm.getOrigin(),
                                                                                  digestAlgorithm.getLast())),
                                    0.125);
        assertNotNull(biffA);
        var bffA = BloomFilter.from(biffA);

        var identifierB = stereotomyB.newIdentifier().get();
        identifierB.rotate().get();
        var digestB = identifierB.getLastEstablishingEvent().get().getCoordinates().getDigest();
        try (var connection = connectionPoolB.getConnection()) {
            KerlDHT.updateLocationHash(identifierB.getIdentifier(), digestAlgorithm, DSL.using(connection));
        }
        var biffB = spaceB.populate(0x1638, new CombinedIntervals(new KeyInterval(digestAlgorithm.getOrigin(),
                                                                                  digestAlgorithm.getLast())),
                                    0.125);
        assertNotNull(biffB);
        var bffB = BloomFilter.from(biffB);

        assertTrue(bffA.contains(digestA));
        assertFalse(bffA.contains(digestB));

        assertTrue(bffB.contains(digestB));
        assertFalse(bffB.contains(digestA));

        assertNull(kerlA.getKeyState(identifierB.getIdentifier()).get());
        assertNull(kerlB.getKeyState(identifierA.getIdentifier()).get());

        var updateA = spaceA.reconcile(Intervals.newBuilder()
                                                .addIntervals(Interval.newBuilder()
                                                                      .setStart(digestAlgorithm.getOrigin().toDigeste())
                                                                      .setEnd(digestAlgorithm.getLast().toDigeste())
                                                                      .build())
                                                .setHave(biffB)
                                                .build(),
                                       kerlA);
        assertNotNull(updateA);
        assertEquals(2, updateA.getEventsCount());

        var updateB = spaceB.reconcile(Intervals.newBuilder()
                                                .addIntervals(Interval.newBuilder()
                                                                      .setStart(digestAlgorithm.getOrigin().toDigeste())
                                                                      .setEnd(digestAlgorithm.getLast().toDigeste())
                                                                      .build())
                                                .setHave(biffA)
                                                .build(),
                                       kerlB);
        assertNotNull(updateB);
        assertEquals(2, updateB.getEventsCount());

        spaceA.update(updateB.getEventsList(), kerlA);
        spaceB.update(updateA.getEventsList(), kerlB);

        assertNotNull(kerlA.getKeyState(identifierB.getIdentifier()).get());
        assertNotNull(kerlB.getKeyState(identifierA.getIdentifier()).get());
    }
}
