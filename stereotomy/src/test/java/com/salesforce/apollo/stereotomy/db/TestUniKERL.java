/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import static com.salesforce.apollo.crypto.SigningThreshold.unweighted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.h2.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Signer.SignerImpl;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.RotationEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification;
import com.salesforce.apollo.stereotomy.identifier.spec.IdentifierSpecification.Builder;
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

        var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
        try (Liquibase liquibase = new Liquibase("/stereotomy/initialize.xml", new ClassLoaderResourceAccessor(), database)) {
            liquibase.update((String) null);
        }
        connection = new JdbcConnection(url, new Properties(), "", "");
        var uni = new UniKERLDirect(connection, DigestAlgorithm.DEFAULT);

        doOne(factory, uni);
        doOne(factory, uni);
        doOne(factory, uni);
        doOne(factory, uni);

        doOne(factory, connection, uni);
        doOne(factory, connection, uni);
        doOne(factory, connection, uni);
        doOne(factory, connection, uni);
    }

    private byte[] append(KeyEvent event, Connection connection) {
        CallableStatement proc;
        try {
            proc = connection.prepareCall("{ ? = call stereotomy.append(?, ?, ?) }");
            proc.registerOutParameter(1, Types.BINARY);
            proc.setObject(2, event.getBytes());
            proc.setObject(3, event.getIlk());
            proc.setObject(4, DigestAlgorithm.DEFAULT.digestCode());
            proc.execute();
            return proc.getBytes(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private void doOne(ProtobufEventFactory factory, Connection connection,
                       UniKERL uni) throws InvalidProtocolBufferException {
        var specification = IdentifierSpecification.newBuilder();
        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);

        var inception = inception(specification, initialKeyPair, factory, nextKeyPair);
        var appendKeyState = new KeyStateImpl(append(inception, connection));

        var retrieved = uni.getKeyEvent(inception.getCoordinates());
        assertNotNull(retrieved);
        assertFalse(retrieved.isEmpty());
        assertEquals(inception, retrieved.get());
        var current = uni.getKeyState(inception.getIdentifier());
        assertNotNull(current);
        assertEquals(inception.getCoordinates(), current.get().getCoordinates());

        assertNotNull(appendKeyState);
        assertEquals(current.get(), appendKeyState);

        // rotate
        var prevNext = nextKeyPair;
        nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        RotationEvent rotation = rotation(inception, nextKeyPair, uni, prevNext, connection, factory);

        // rotate again
        prevNext = nextKeyPair;
        nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        rotation = rotation(rotation, nextKeyPair, uni, prevNext, connection, factory);

        // rotate once more
        prevNext = nextKeyPair;
        nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        rotation = rotation(rotation, nextKeyPair, uni, prevNext, connection, factory);
    }

    private void doOne(ProtobufEventFactory factory, UniKERLDirect uni) {
        var specification = IdentifierSpecification.newBuilder();
        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);

        var inception = inception(specification, initialKeyPair, factory, nextKeyPair);
        uni.append(inception);

        var retrieved = uni.getKeyEvent(inception.getCoordinates());
        assertNotNull(retrieved);
        assertFalse(retrieved.isEmpty());
        assertEquals(inception, retrieved.get());
        var current = uni.getKeyState(inception.getIdentifier());
        assertNotNull(current);
        assertEquals(inception.getCoordinates(), current.get().getCoordinates());

        // rotate
        var prevNext = nextKeyPair;
        nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        RotationEvent rotation = rotation(inception, prevNext, uni, factory, nextKeyPair);

        // rotate again
        prevNext = nextKeyPair;
        nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        rotation = rotation(rotation, prevNext, uni, factory, nextKeyPair);

        // rotate once more
        prevNext = nextKeyPair;
        nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        rotation = rotation(rotation, prevNext, uni, factory, nextKeyPair);
    }

    private InceptionEvent inception(Builder specification, KeyPair initialKeyPair, ProtobufEventFactory factory,
                                     KeyPair nextKeyPair) {

        specification.addKey(initialKeyPair.getPublic()).setSigningThreshold(unweighted(1))
                     .setNextKeys(List.of(nextKeyPair.getPublic())).setWitnesses(Collections.emptyList())
                     .setSigner(new SignerImpl(initialKeyPair.getPrivate()));
        var identifier = Identifier.NONE;
        InceptionEvent event = factory.inception(identifier, specification.build());
        return event;
    }

    private RotationEvent rotation(EstablishmentEvent event, KeyPair nextKeyPair, UniKERL uni, KeyPair prevNext,
                                   Connection connection,
                                   ProtobufEventFactory factory) throws InvalidProtocolBufferException {
        var digest = event.hash(uni.getDigestAlgorithm());

        RotationEvent rotation = rotation(prevNext, digest, event, nextKeyPair, factory);
        var appendKeyState = new KeyStateImpl(append(rotation, connection));
        assertNotNull(appendKeyState);

        var retrieved = uni.getKeyEvent(rotation.getCoordinates());
        assertNotNull(retrieved);
        assertFalse(retrieved.isEmpty());
        assertEquals(rotation, retrieved.get());
        var current = uni.getKeyState(rotation.getIdentifier());
        assertNotNull(current);
        assertEquals(rotation.getCoordinates(), current.get().getCoordinates());
        assertEquals(current.get(), appendKeyState);
        return rotation;
    }

    private RotationEvent rotation(EstablishmentEvent event, KeyPair prevNext, UniKERLDirect uni,
                                   ProtobufEventFactory factory, KeyPair nextKeyPair) {
        var digest = event.hash(uni.getDigestAlgorithm());

        RotationEvent rotation = rotation(prevNext, digest, event, nextKeyPair, factory);
        uni.append(rotation);

        var retrieved = uni.getKeyEvent(rotation.getCoordinates());
        assertNotNull(retrieved);
        assertFalse(retrieved.isEmpty());
        assertEquals(rotation, retrieved.get());
        var current = uni.getKeyState(rotation.getIdentifier());
        assertNotNull(current);
        assertEquals(rotation.getCoordinates(), current.get().getCoordinates());
        return rotation;
    }

    private RotationEvent rotation(KeyPair prevNext, final Digest prevDigest, EstablishmentEvent prev,
                                   KeyPair nextKeyPair, ProtobufEventFactory factory) {
        var rotSpec = RotationSpecification.newBuilder();
        rotSpec.setIdentifier(prev.getIdentifier()).setCurrentCoords(prev.getCoordinates()).setCurrentDigest(prevDigest)
               .setKey(prevNext.getPublic()).setSigningThreshold(unweighted(1))
               .setNextKeys(List.of(nextKeyPair.getPublic())).setSigner(new SignerImpl(prevNext.getPrivate()));

        RotationEvent rotation = factory.rotation(rotSpec.build());
        return rotation;
    }
}
