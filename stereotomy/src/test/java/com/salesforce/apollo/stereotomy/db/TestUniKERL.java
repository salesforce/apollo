/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import static com.salesforce.apollo.stereotomy.event.SigningThreshold.unweighted;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.h2.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
import com.salesforce.apollo.stereotomy.identifier.spec.KeyConfigurationDigester;
import com.salesforce.apollo.stereotomy.identifier.spec.RotationSpecification;

import liquibase.Liquibase;
import liquibase.database.core.H2Database;
import liquibase.resource.ClassLoaderResourceAccessor;

/**
 * @author hal.hildebrand
 *
 */
public class TestUniKERL {
    private static final SecureRandom entropy = new SecureRandom();

    @Test
    public void smoke() throws Exception {
        var factory = new ProtobufEventFactory();
        final var url = "jdbc:h2:mem:test_engine-smoke;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";
        var connection = new JdbcConnection(url, new Properties(), "", "");

        final var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
        try (Liquibase liquibase = new Liquibase("/stereotomy.xml", new ClassLoaderResourceAccessor(), database)) {
            liquibase.update((String) null);
        }
        connection = new JdbcConnection(url, new Properties(), "", "");
        var uni = new UniKERLDirect(connection, DigestAlgorithm.DEFAULT);
        uni.initialize();

        var specification = IdentifierSpecification.newBuilder();
        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);

        var inception = inception(specification, initialKeyPair, factory, nextKeyPair);
        uni.append(inception, null);

        // rotate
        nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var digest = inception.hash(uni.getDigestAlgorithm());

        RotationEvent rotation = rotation(digest, inception, nextKeyPair, factory);
        uni.append(rotation, null);

        // rotate again
        nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        digest = inception.hash(uni.getDigestAlgorithm());

        rotation = rotation(digest, rotation, nextKeyPair, factory);
        uni.append(rotation, null);
    }

    private InceptionEvent inception(Builder specification, KeyPair initialKeyPair, ProtobufEventFactory factory,
                                     KeyPair nextKeyPair) {

        var nextKeys = KeyConfigurationDigester.digest(unweighted(1), List.of(nextKeyPair.getPublic()),
                                                       specification.getNextKeysAlgorithm());

        specification.setKey(initialKeyPair.getPublic()).setNextKeys(nextKeys).setWitnesses(Collections.emptyList())
                     .setSigner(0, initialKeyPair.getPrivate()).build();
        var identifier = Identifier.NONE;
        InceptionEvent event = factory.inception(identifier, specification.build());
        return event;
    }

    private RotationEvent rotation(final Digest prevDigest, EstablishmentEvent prev, KeyPair nextKeyPair,
                                   ProtobufEventFactory factory) {
        var rotSpec = RotationSpecification.newBuilder();
        Digest nextKeys = KeyConfigurationDigester.digest(unweighted(1), List.of(nextKeyPair.getPublic()),
                                                          rotSpec.getNextKeysAlgorithm());
        rotSpec.setIdentifier(prev.getIdentifier()).setCurrentCoords(prev.getCoordinates()).setCurrentDigest(prevDigest)
               .setKey(nextKeyPair.getPublic()).setNextKeys(nextKeys).setSigner(0, nextKeyPair.getPrivate());

        RotationEvent rotation = factory.rotation(rotSpec.build());
        return rotation;
    }
}
