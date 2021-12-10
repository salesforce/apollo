/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import static com.salesforce.apollo.model.schema.tables.Coordinates.COORDINATES;
import static com.salesforce.apollo.model.schema.tables.Event.EVENT;
import static com.salesforce.apollo.model.schema.tables.Identifier.IDENTIFIER;
import static com.salesforce.apollo.stereotomy.event.KeyEvent.DELEGATED_INCEPTION_TYPE;
import static com.salesforce.apollo.stereotomy.event.KeyEvent.DELEGATED_ROTATION_TYPE;
import static com.salesforce.apollo.stereotomy.event.KeyEvent.INCEPTION_TYPE;
import static com.salesforce.apollo.stereotomy.event.KeyEvent.INTERACTION_TYPE;
import static com.salesforce.apollo.stereotomy.event.KeyEvent.ROTATION_TYPE;

import java.sql.Connection;
import java.util.Optional;
import java.util.OptionalLong;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.salesfoce.apollo.stereotomy.event.proto.InceptionEvent;
import com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent;
import com.salesfoce.apollo.stereotomy.event.proto.RotationEvent;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.DelegatingEventCoordinates;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.SealingEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.DelegatedInceptionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.DelegatedRotationEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.InceptionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.InteractionEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.RotationEventImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
abstract public class UniKERL implements KERL {

    protected final DigestAlgorithm digestAlgorithm;
    protected final DSLContext      dsl;

    public UniKERL(Connection connection, DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        this.dsl = DSL.using(connection);
    }

    @Override
    public OptionalLong findLatestReceipt(Identifier forIdentifier, Identifier byIdentifier) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    @Override
    public Optional<SealingEvent> getKeyEvent(DelegatingEventCoordinates coordinates) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(Digest digest) {
        return dsl.select(EVENT.CONTENT, COORDINATES.ILK).from(EVENT).join(COORDINATES)
                  .on(COORDINATES.ID.eq(EVENT.COORDINATES))
                  .where(EVENT.DIGEST.eq(digest.toDigeste().toByteString().toByteArray())).fetchOptional()
                  .map(r -> mapKeyEvent(r.value1(), r.value2()));
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return dsl.select(EVENT.CONTENT, COORDINATES.ILK).from(EVENT).join(COORDINATES)
                  .on(EVENT.COORDINATES.eq(COORDINATES.ID)).join(IDENTIFIER)
                  .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toIdent().toByteArray()))
                  .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                  .and(COORDINATES.DIGEST.eq(coordinates.getDigest().toDigeste().toByteArray()))
                  .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber()))
                  .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                  .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber())).fetchOptional()
                  .map(r -> mapKeyEvent(r.value1(), r.value2()));
    }

    @Override
    public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Optional<KeyState> getKeyState(Identifier identifier) {
        // TODO Auto-generated method stub
        return null;
    }

    private KeyEvent mapKeyEvent(byte[] event, String ilk) {
        try {
            return switch (ilk) {
            case ROTATION_TYPE -> new RotationEventImpl(RotationEvent.parseFrom(event));
            case DELEGATED_INCEPTION_TYPE -> new DelegatedInceptionEventImpl(InceptionEvent.parseFrom(event));
            case DELEGATED_ROTATION_TYPE -> new DelegatedRotationEventImpl(RotationEvent.parseFrom(event));
            case INCEPTION_TYPE -> new InceptionEventImpl(InceptionEvent.parseFrom(event));
            case INTERACTION_TYPE -> new InteractionEventImpl(InteractionEvent.parseFrom(event));
            default -> null;
            };
        } catch (Throwable e) {
            return null;
        }
    }

}
