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
import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.toKeyEvent;

import java.sql.Connection;
import java.util.Optional;
import java.util.OptionalLong;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.DelegatingEventCoordinates;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.SealingEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
abstract public class UniKERL implements KERL {
    private static final byte[] DIGEST_NONE_BYTES = Digest.NONE.toDigeste().toByteArray();
    private static final Logger log               = LoggerFactory.getLogger(UniKERL.class);

    public static void append(DSLContext dsl, KeyEvent event, KeyState newState, DigestAlgorithm digestAlgorithm) {
        dsl.transaction(ctx -> {
            var context = DSL.using(ctx);
            final EventCoordinates prevCoords = event.getPrevious();
            final var preIdentifier = context.select(IDENTIFIER.ID).from(IDENTIFIER)
                                             .where(IDENTIFIER.PREFIX.eq(prevCoords.getIdentifier().toIdent()
                                                                                   .toByteArray()));
            final var prev = context.select(COORDINATES.ID).from(COORDINATES)
                                    .where(COORDINATES.DIGEST.eq(prevCoords.getDigest().toDigeste().toByteArray()))
                                    .and(COORDINATES.IDENTIFIER.eq(preIdentifier))
                                    .and(COORDINATES.SEQUENCE_NUMBER.eq(prevCoords.getSequenceNumber()))
                                    .and(COORDINATES.ILK.eq(prevCoords.getIlk())).fetchOne();
            if (prev == null) {
                log.error("Cannot find previous coordinates: {}", prevCoords);
                throw new IllegalArgumentException("Cannot find previous coordinates: " + prevCoords
                + " for inserted event");
            }
            final var prevDigest = context.select(EVENT.DIGEST).from(EVENT).where(EVENT.COORDINATES.eq(prev.value1()))
                                          .fetchOne();

            final var identBytes = event.getIdentifier().toIdent().toByteArray();
            context.mergeInto(IDENTIFIER).using(context.selectOne()).on(IDENTIFIER.PREFIX.eq(identBytes))
                   .whenNotMatchedThenInsert(IDENTIFIER.PREFIX).values(identBytes).execute();

            final var id = context.insertInto(COORDINATES)
                                  .set(COORDINATES.DIGEST, prevDigest == null ? DIGEST_NONE_BYTES : prevDigest.value1())
                                  .set(COORDINATES.IDENTIFIER,
                                       context.select(IDENTIFIER.ID).from(IDENTIFIER)
                                              .where(IDENTIFIER.PREFIX.eq(identBytes)))
                                  .set(COORDINATES.ILK, event.getIlk())
                                  .set(COORDINATES.SEQUENCE_NUMBER, event.getSequenceNumber())
                                  .returningResult(COORDINATES.ID).fetchOne().value1();
            final var digest = event.hash(digestAlgorithm);
            context.insertInto(EVENT).set(EVENT.COORDINATES, id).set(EVENT.DIGEST, digest.toDigeste().toByteArray())
                   .set(EVENT.CONTENT, event.getBytes()).set(EVENT.PREVIOUS, prev.value1()).execute();
        });
    }

    public static void initialize(DSLContext dsl) {
        dsl.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.insertInto(IDENTIFIER).set(IDENTIFIER.ID, 0L)
                   .set(IDENTIFIER.PREFIX, EventCoordinates.NONE.getIdentifier().toIdent().toByteArray()).execute();
            context.insertInto(COORDINATES).set(COORDINATES.ID, 0L)
                   .set(COORDINATES.DIGEST, EventCoordinates.NONE.getDigest().toDigeste().toByteArray())
                   .set(COORDINATES.IDENTIFIER, 0L)
                   .set(COORDINATES.SEQUENCE_NUMBER, EventCoordinates.NONE.getSequenceNumber())
                   .set(COORDINATES.ILK, EventCoordinates.NONE.getIlk()).execute();
        });
    }

    protected final DigestAlgorithm digestAlgorithm;

    protected final DSLContext dsl;

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
                  .map(r -> toKeyEvent(r.value1(), r.value2()));
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
                  .map(r -> toKeyEvent(r.value1(), r.value2()));
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

}
