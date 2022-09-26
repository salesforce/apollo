/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.h2.jdbc.JdbcConnection;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.caching.CachingKERL;
import com.salesforce.apollo.stereotomy.db.UniKERLDirectPooled;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

import liquibase.Liquibase;
import liquibase.database.core.H2Database;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * @author hal.hildebrand
 *
 */
public class AniTest extends AbstractDhtTest {

    @Test
    public void singleAni() throws Exception {
        var timeout = Duration.ofSeconds(1000);
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        SigningThreshold threshold = SigningThreshold.unweighted(3);

        final var url = "jdbc:h2:mem:test_engine-smoke;DB_CLOSE_DELAY=-1";
        JdbcConnection connection = new JdbcConnection(url, new Properties(), "", "", false);

        var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
        try (Liquibase liquibase = new Liquibase("/stereotomy/initialize.xml", new ClassLoaderResourceAccessor(),
                                                 database)) {
            liquibase.update((String) null);
        }
        JdbcConnectionPool connectionPool = JdbcConnectionPool.create(url, "", "");
        connectionPool.setMaxConnections(10);
        final var pooled = new UniKERLDirectPooled(connectionPool, DigestAlgorithm.DEFAULT);
        KERL kerl = new CachingKERL(f -> {
            try (var k = pooled.create()) {
                return f.apply(k);
            } catch (Throwable e) {
                throw new IllegalStateException(e);
            }
        });
        var controller = new StereotomyImpl(new MemKeyStore(), kerl, entropy);

        var v1 = controller.newIdentifier().get();
        var v2 = controller.newIdentifier().get();
        var v3 = controller.newIdentifier().get();
        var roots = new DigestBloomFilter(entropy.nextLong(), 100, 0.00125);
        for (var id : List.of(v1.getIdentifier(), v2.getIdentifier(), v3.getIdentifier())) {
            roots.add(id.getDigest());
        }

        var ani = new Ani(identities.keySet().stream().findFirst().get(), timeout, kerl, () -> threshold, () -> roots,
                          () -> SigningThreshold.unweighted(3));

        // inception
        var identifier = controller.newIdentifier().get();
        var inception = identifier.getLastEstablishingEvent().get();

        assertFalse(ani.validateRoot(inception).get(10, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));
        var validations = new HashMap<EventCoordinates, JohnHancock>();

        ani.clearValidations();
        validations.put(v1.getCoordinates(), v1.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        var retrieved = kerl.getValidations(inception.getCoordinates()).get();
        assertEquals(1, retrieved.size());

        assertFalse(ani.validateRoot(inception).get(10, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));

        ani.clearValidations();
        validations.put(v2.getCoordinates(), v2.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        retrieved = kerl.getValidations(inception.getCoordinates()).get();
        assertEquals(2, retrieved.size());

        assertFalse(ani.validateRoot(inception).get(10, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));

        ani.clearValidations();
        validations.put(v3.getCoordinates(), v3.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        retrieved = kerl.getValidations(inception.getCoordinates()).get();
        assertEquals(3, retrieved.size());

        assertTrue(ani.validateRoot(inception).get(10, TimeUnit.SECONDS));
        assertTrue(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));
    }

    @Test
    public void smokin() throws Exception {
        var timeout = Duration.ofSeconds(1000);
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        SigningThreshold threshold = SigningThreshold.unweighted(0);
        routers.values().forEach(lr -> lr.start());
        dhts.values().forEach(e -> e.start(Executors.newSingleThreadScheduledExecutor(), Duration.ofSeconds(1)));

        var dht = dhts.values().stream().findFirst().get();
        var roots = new DigestBloomFilter(entropy.nextLong(), 100, 0.00125);

        Map<SigningMember, Ani> anis = dhts.entrySet()
                                           .stream()
                                           .collect(Collectors.toMap(e -> e.getKey(),
                                                                     e -> new Ani(e.getKey(), timeout,
                                                                                  dhts.get(e.getKey()).asKERL(),
                                                                                  () -> threshold, () -> roots,
                                                                                  () -> threshold)));
        var ani = anis.values().stream().findFirst().get();

        // inception
        var specification = IdentifierSpecification.newBuilder();
        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var inception = inception(specification, initialKeyPair, factory, nextKeyPair);

        dht.append(Collections.singletonList(inception.toKeyEvent_())).get();
        ani.validateRoot(inception).get();
        final var success = ani.validateRoot(inception).get(10, TimeUnit.SECONDS);
        assertTrue(success);
        assertTrue(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));
    }

    @Test
    public void threshold() throws Exception {
        var timeout = Duration.ofSeconds(1000);
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        SigningThreshold threshold = SigningThreshold.unweighted(3);
        routers.values().forEach(lr -> lr.start());
        dhts.values().forEach(e -> e.start(Executors.newSingleThreadScheduledExecutor(), Duration.ofSeconds(1)));

        var kerl = dhts.values().stream().findFirst().get().asKERL();
        var controller = new StereotomyImpl(new MemKeyStore(), kerl, entropy);

        var v1 = controller.newIdentifier().get();
        var v2 = controller.newIdentifier().get();
        var v3 = controller.newIdentifier().get();

        var roots = new DigestBloomFilter(entropy.nextLong(), 100, 0.00125);
        for (var id : List.of(v1.getIdentifier(), v2.getIdentifier(), v3.getIdentifier())) {
            roots.add(id.getDigest());
        }

        Map<SigningMember, Ani> anis = dhts.entrySet()
                                           .stream()
                                           .collect(Collectors.toMap(e -> e.getKey(),
                                                                     e -> new Ani(e.getKey(), timeout,
                                                                                  dhts.get(e.getKey()).asKERL(),
                                                                                  () -> threshold, () -> roots,
                                                                                  () -> threshold)));
        var ani = anis.values().stream().findFirst().get();

        // inception
        var identifier = controller.newIdentifier().get();
        var inception = identifier.getLastEstablishingEvent().get();

        assertFalse(ani.validateRoot(inception).get(5, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(5)).validate(inception));
        var validations = new HashMap<EventCoordinates, JohnHancock>();

        ani.clearValidations();
        validations.put(v1.getCoordinates(), v1.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        var retrieved = kerl.getValidations(inception.getCoordinates()).get();
        assertEquals(1, retrieved.size());

        assertFalse(ani.validateRoot(inception).get(5, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(5)).validate(inception));

        ani.clearValidations();
        validations.put(v2.getCoordinates(), v2.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        retrieved = kerl.getValidations(inception.getCoordinates()).get();
        assertEquals(2, retrieved.size());

        assertFalse(ani.validateRoot(inception).get(120, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(5)).validate(inception));

        ani.clearValidations();
        validations.put(v3.getCoordinates(), v3.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        var condition = ani.validateRoot(inception).get();
        assertTrue(condition);
        assertTrue(ani.eventValidation(Duration.ofSeconds(5)).validate(inception));
    }
}
