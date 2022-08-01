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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.h2.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.StereotomyImpl;
import com.salesforce.apollo.stereotomy.db.UniKERLDirect;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.mem.MemKeyStore;

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
        var connection = new JdbcConnection(url, new Properties(), "", "", false);

        var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
        try (Liquibase liquibase = new Liquibase("/stereotomy/initialize.xml", new ClassLoaderResourceAccessor(),
                                                 database)) {
            liquibase.update((String) null);
        }
        connection = new JdbcConnection(url, new Properties(), "", "", false);
        var kerl = new UniKERLDirect(connection, DigestAlgorithm.DEFAULT);
        var ani = new Ani(identities.keySet().stream().findFirst().get(), threshold, timeout, kerl);
        var controller = new StereotomyImpl(new MemKeyStore(), kerl, entropy);

        // inception
        var identifier = controller.newIdentifier().get();
        var inception = identifier.getLastEstablishingEvent().get();

        assertFalse(ani.validate(inception).get(10, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));

        var v1 = controller.newIdentifier().get();
        var v2 = controller.newIdentifier().get();
        var v3 = controller.newIdentifier().get();
        var validations = new HashMap<Identifier, JohnHancock>();

        ani.clearValidations();
        validations.put(v1.getIdentifier(), v1.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        var retrieved = kerl.getValidations(inception.getCoordinates()).get();
        assertEquals(1, retrieved.size());

        assertFalse(ani.validate(inception).get(10, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));

        ani.clearValidations();
        validations.put(v2.getIdentifier(), v2.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        retrieved = kerl.getValidations(inception.getCoordinates()).get();
        assertEquals(2, retrieved.size());

        assertFalse(ani.validate(inception).get(10, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));

        ani.clearValidations();
        validations.put(v3.getIdentifier(), v3.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        assertTrue(ani.validate(inception).get(10, TimeUnit.SECONDS));
        assertTrue(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));
    }

    @Test
    public void smokin() throws Exception {
        var timeout = Duration.ofSeconds(10);
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        SigningThreshold threshold = SigningThreshold.unweighted(0);

        Map<SigningMember, Ani> anis = dhts.entrySet()
                                           .stream()
                                           .collect(Collectors.toMap(e -> e.getKey(),
                                                                     e -> new Ani(e.getKey(), threshold, timeout,
                                                                                  dhts.get(e.getKey()).asKERL())));
        routers.values().forEach(lr -> lr.start());
        dhts.values().forEach(e -> e.start(Executors.newSingleThreadScheduledExecutor(), Duration.ofSeconds(1)));

        var dht = dhts.values().stream().findFirst().get();
        var ani = anis.values().stream().findFirst().get();

        // inception
        var specification = IdentifierSpecification.newBuilder();
        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var inception = inception(specification, initialKeyPair, factory, nextKeyPair);

        dht.append(Collections.singletonList(inception.toKeyEvent_())).get();
        ani.validate(inception).get();
        final var success = ani.validate(inception).get(10, TimeUnit.SECONDS);
        assertTrue(success);
        assertTrue(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));
    }

    @Test
    public void threshold() throws Exception {
        var timeout = Duration.ofSeconds(1000);
        var entropy = SecureRandom.getInstance("SHA1PRNG");
        entropy.setSeed(new byte[] { 6, 6, 6 });

        SigningThreshold threshold = SigningThreshold.unweighted(3);

        Map<SigningMember, Ani> anis = dhts.entrySet()
                                           .stream()
                                           .collect(Collectors.toMap(e -> e.getKey(),
                                                                     e -> new Ani(e.getKey(), threshold, timeout,
                                                                                  dhts.get(e.getKey()).asKERL())));
        routers.values().forEach(lr -> lr.start());
        dhts.values().forEach(e -> e.start(Executors.newSingleThreadScheduledExecutor(), Duration.ofSeconds(1)));

        var ani = anis.values().stream().findFirst().get();
        var kerl = ani.getKerl();
        var controller = new StereotomyImpl(new MemKeyStore(), kerl, entropy);

        // inception
        var identifier = controller.newIdentifier().get();
        var inception = identifier.getLastEstablishingEvent().get();

        assertFalse(ani.validate(inception).get(10, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));

        var v1 = controller.newIdentifier().get();
        var v2 = controller.newIdentifier().get();
        var v3 = controller.newIdentifier().get();
        var validations = new HashMap<Identifier, JohnHancock>();

        ani.clearValidations();
        validations.put(v1.getIdentifier(), v1.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        var retrieved = kerl.getValidations(inception.getCoordinates()).get();
        assertEquals(1, retrieved.size());

        assertFalse(ani.validate(inception).get(10, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));

        ani.clearValidations();
        validations.put(v2.getIdentifier(), v2.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        retrieved = kerl.getValidations(inception.getCoordinates()).get();
        assertEquals(2, retrieved.size());

        assertFalse(ani.validate(inception).get(10, TimeUnit.SECONDS));
        assertFalse(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));

        ani.clearValidations();
        validations.put(v3.getIdentifier(), v3.getSigner().get().sign(inception.toKeyEvent_().toByteString()));
        kerl.appendValidations(inception.getCoordinates(), validations).get();

        assertTrue(ani.validate(inception).get(10, TimeUnit.SECONDS));
        assertTrue(ani.eventValidation(Duration.ofSeconds(10)).validate(inception));
    }
}
