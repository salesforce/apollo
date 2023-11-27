/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.Signer.SignerImpl;
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
import org.h2.jdbc.JdbcConnection;
import org.joou.ULong;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.SecureRandom;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static com.salesforce.apollo.cryptography.SigningThreshold.unweighted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author hal.hildebrand
 */
public class TestUniKERL {
    private static final SecureRandom entropy = new SecureRandom();

    @Test
    public void smoke() throws Exception {
        var factory = new ProtobufEventFactory();
        final var url = "jdbc:h2:mem:test_engine-smoke;DB_CLOSE_DELAY=-1";
        var connection = new JdbcConnection(url, new Properties(), "", "", false);

        var database = new H2Database();
        database.setConnection(new liquibase.database.jvm.JdbcConnection(connection));
        try (Liquibase liquibase = new Liquibase("/stereotomy/initialize.xml", new ClassLoaderResourceAccessor(),
                                                 database)) {
            liquibase.update((String) null);
        }
        connection = new JdbcConnection(url, new Properties(), "", "", false);
        var uni = new UniKERLDirect(connection, DigestAlgorithm.DEFAULT);

        doOne(factory, uni);
        doOne(factory, uni);
        doOne(factory, uni);
        doOne(factory, uni);

        doOne(factory, connection, uni);
        doOne(factory, connection, uni);
        doOne(factory, connection, uni);
        doOne(factory, connection, uni);

        var statement = connection.createStatement();
        statement.execute("select content from stereotomy.event");
        var result = statement.getResultSet();
        int sum = 0;
        int count = 0;
        while (result.next()) {
            sum += result.getBytes(1).length;
            count++;
        }
        System.out.println("Average size: " + sum / count);
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

    private void doOne(ProtobufEventFactory factory, Connection connection, UniKERL uni) throws Exception {
        var specification = IdentifierSpecification.newBuilder();
        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);

        var inception = inception(specification, initialKeyPair, factory, nextKeyPair);
        var appendKeyState = new KeyStateImpl(append(inception, connection));

        var retrieved = uni.getKeyEvent(inception.getCoordinates());
        assertNotNull(retrieved);
        assertEquals(inception, retrieved);
        var current = uni.getKeyState(inception.getIdentifier());
        assertNotNull(current);
        assertEquals(inception.getCoordinates(), current.getCoordinates());

        assertNotNull(appendKeyState);
        assertEquals(current, appendKeyState);

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

    private void doOne(ProtobufEventFactory factory, UniKERLDirect uni) throws Exception {
        var specification = IdentifierSpecification.newBuilder();
        var initialKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);
        var nextKeyPair = specification.getSignatureAlgorithm().generateKeyPair(entropy);

        var inception = inception(specification, initialKeyPair, factory, nextKeyPair);
        uni.append(inception);

        var retrieved = uni.getKeyEvent(inception.getCoordinates());
        assertNotNull(retrieved);
        assertEquals(inception, retrieved);
        var current = uni.getKeyState(inception.getIdentifier());
        assertNotNull(current);
        assertEquals(inception.getCoordinates(), current.getCoordinates());

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

    private InceptionEvent inception(Builder<?> specification, KeyPair initialKeyPair, ProtobufEventFactory factory,
                                     KeyPair nextKeyPair) {

        specification.addKey(initialKeyPair.getPublic())
                     .setSigningThreshold(unweighted(1))
                     .setNextKeys(List.of(nextKeyPair.getPublic()))
                     .setWitnesses(Collections.emptyList())
                     .setSigner(new SignerImpl(initialKeyPair.getPrivate(), ULong.MIN));
        var identifier = Identifier.NONE;
        InceptionEvent event = factory.inception(identifier, specification.build());
        return event;
    }

    private RotationEvent rotation(EstablishmentEvent event, KeyPair nextKeyPair, UniKERL uni, KeyPair prevNext,
                                   Connection connection, ProtobufEventFactory factory) throws Exception {
        var digest = event.hash(uni.getDigestAlgorithm());

        RotationEvent rotation = rotation(prevNext, digest, event, nextKeyPair, factory);
        var appendKeyState = new KeyStateImpl(append(rotation, connection));
        assertNotNull(appendKeyState);

        var retrieved = uni.getKeyEvent(rotation.getCoordinates());
        assertNotNull(retrieved);
        assertEquals(rotation, retrieved);
        var current = uni.getKeyState(rotation.getIdentifier());
        assertNotNull(current);
        assertEquals(rotation.getCoordinates(), current.getCoordinates());
        assertEquals(current, appendKeyState);
        return rotation;
    }

    private RotationEvent rotation(EstablishmentEvent event, KeyPair prevNext, UniKERLDirect uni,
                                   ProtobufEventFactory factory, KeyPair nextKeyPair) throws Exception {
        var digest = event.hash(uni.getDigestAlgorithm());

        RotationEvent rotation = rotation(prevNext, digest, event, nextKeyPair, factory);
        uni.append(rotation);

        var retrieved = uni.getKeyEvent(rotation.getCoordinates());
        assertNotNull(retrieved);
        assertEquals(rotation, retrieved);
        var current = uni.getKeyState(rotation.getIdentifier());
        assertNotNull(current);
        assertEquals(rotation.getCoordinates(), current.getCoordinates());
        return rotation;
    }

    private RotationEvent rotation(KeyPair prevNext, final Digest prevDigest, EstablishmentEvent prev,
                                   KeyPair nextKeyPair, ProtobufEventFactory factory) {
        var rotSpec = RotationSpecification.newBuilder();
        rotSpec.setIdentifier(prev.getIdentifier())
               .setCurrentCoords(prev.getCoordinates())
               .setCurrentDigest(prevDigest)
               .setKey(prevNext.getPublic())
               .setSigningThreshold(unweighted(1))
               .setNextKeys(List.of(nextKeyPair.getPublic()))
               .setSigner(new SignerImpl(prevNext.getPrivate(), ULong.MIN));

        RotationEvent rotation = factory.rotation(rotSpec.build(), false);
        return rotation;
    }
}
