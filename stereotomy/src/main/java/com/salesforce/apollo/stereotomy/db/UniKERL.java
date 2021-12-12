/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import static com.salesforce.apollo.model.schema.tables.Coordinates.COORDINATES;
import static com.salesforce.apollo.model.schema.tables.CurrentKeyState.CURRENT_KEY_STATE;
import static com.salesforce.apollo.model.schema.tables.Event.EVENT;
import static com.salesforce.apollo.model.schema.tables.Identifier.IDENTIFIER;
import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.toKeyEvent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.DelegatingEventCoordinates;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.SealingEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.processing.KeyEventProcessor;

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
            final var identifierId = context.select(IDENTIFIER.ID).from(IDENTIFIER)
                                            .where(IDENTIFIER.PREFIX.eq(identBytes)).fetchOne().value1();

            final var id = context.insertInto(COORDINATES)
                                  .set(COORDINATES.DIGEST, prevDigest == null ? DIGEST_NONE_BYTES : prevDigest.value1())
                                  .set(COORDINATES.IDENTIFIER,
                                       context.select(IDENTIFIER.ID).from(IDENTIFIER)
                                              .where(IDENTIFIER.PREFIX.eq(identBytes)))
                                  .set(COORDINATES.ILK, event.getIlk())
                                  .set(COORDINATES.SEQUENCE_NUMBER, event.getSequenceNumber())
                                  .returningResult(COORDINATES.ID).fetchOne().value1();

            final var leCoords = newState.getLastEstablishmentEvent();

            final var lastEstablishing = context.select(COORDINATES.ID).from(COORDINATES)
                                                .where(COORDINATES.DIGEST.eq(leCoords.getDigest().toDigeste()
                                                                                     .toByteArray()))
                                                .and(COORDINATES.IDENTIFIER.eq(preIdentifier))
                                                .and(COORDINATES.SEQUENCE_NUMBER.eq(leCoords.getSequenceNumber()))
                                                .and(COORDINATES.ILK.eq(leCoords.getIlk())).fetchOne();
            final var digest = event.hash(digestAlgorithm);
            context.insertInto(EVENT).set(EVENT.COORDINATES, id).set(EVENT.DIGEST, digest.toDigeste().toByteArray())
                   .set(EVENT.CONTENT, compress(event.getBytes())).set(EVENT.PREVIOUS, prev.value1())
                   .set(EVENT.LAST_ESTABLISHING_EVENT, lastEstablishing == null ? null : lastEstablishing.value1())
                   .set(EVENT.CURRENT_STATE, compress(newState.getBytes())).execute();

            context.mergeInto(CURRENT_KEY_STATE).using(context.selectOne())
                   .on(CURRENT_KEY_STATE.IDENTIFIER.eq(identifierId)).whenMatchedThenUpdate()
                   .set(CURRENT_KEY_STATE.CURRENT, id)
                   .whenNotMatchedThenInsert(CURRENT_KEY_STATE.IDENTIFIER, CURRENT_KEY_STATE.CURRENT)
                   .values(identifierId, id).execute();
        });
    }

    public static byte[] appendEvent(Connection connection, byte[] event, String ilk, int digestCode) {
        final var uni = new UniKERLDirect(connection, DigestAlgorithm.fromDigestCode(digestCode));
        return uni.append(ProtobufEventFactory.toKeyEvent(event, ilk)).getBytes();
    }

    public static byte[] compress(byte[] input) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzos = new GZIPOutputStream(baos);
        ByteArrayInputStream bais = new ByteArrayInputStream(input);) {
            bais.transferTo(gzos);
            gzos.finish();
            gzos.flush();
            baos.flush();
        } catch (IOException e) {
            throw new IllegalStateException("unable to compress input bytes", e);
        }
        return baos.toByteArray();
    }

    public static byte[] decompress(byte[] input) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(input);
        GZIPInputStream gis = new GZIPInputStream(bais);) {
            gis.transferTo(baos);
            baos.flush();
        } catch (IOException e) {
            throw new IllegalStateException("unable to decompress input bytes", e);
        }
        return baos.toByteArray();
    }

    public static void initialize(DSLContext dsl) {
        dsl.transaction(ctx -> {
            var context = DSL.using(ctx);
            final var ecNone = EventCoordinates.NONE;

            context.mergeInto(IDENTIFIER).using(context.selectOne()).on(IDENTIFIER.ID.eq(0L))
                   .whenNotMatchedThenInsert(IDENTIFIER.ID, IDENTIFIER.PREFIX)
                   .values(0L, ecNone.getIdentifier().toIdent().toByteArray()).execute();

            context.mergeInto(COORDINATES).using(context.selectOne()).on(COORDINATES.ID.eq(0L))
                   .whenNotMatchedThenInsert(COORDINATES.ID, COORDINATES.DIGEST, COORDINATES.IDENTIFIER,
                                             COORDINATES.SEQUENCE_NUMBER, COORDINATES.ILK)
                   .values(0L, ecNone.getDigest().toDigeste().toByteArray(), 0L, ecNone.getSequenceNumber(),
                           ecNone.getIlk())
                   .execute();
        });
    }

    public static void initializeKERL(Connection connection) {
        initialize(DSL.using(connection));
    }

    protected final DigestAlgorithm digestAlgorithm;
    protected final DSLContext      dsl;

    private final KeyEventProcessor processor;

    public UniKERL(Connection connection, DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        this.dsl = DSL.using(connection);
        processor = new KeyEventProcessor(this);
    }

    public KeyState append(KeyEvent k) {
        return processor.process(k);
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
                  .map(r -> toKeyEvent(decompress(r.value1()), r.value2()));
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
                  .map(r -> toKeyEvent(decompress(r.value1()), r.value2()));
    }

    @Override
    public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
        return dsl.select(EVENT.CURRENT_STATE).from(EVENT).join(COORDINATES).on(EVENT.COORDINATES.eq(COORDINATES.ID))
                  .join(IDENTIFIER).on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toIdent().toByteArray()))
                  .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                  .and(COORDINATES.DIGEST.eq(coordinates.getDigest().toDigeste().toByteArray()))
                  .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber()))
                  .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                  .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber())).fetchOptional().map(r -> {
                      try {
                          return new KeyStateImpl(decompress(r.value1()));
                      } catch (InvalidProtocolBufferException e) {
                          return null;
                      }
                  });
    }

    @Override
    public Optional<KeyState> getKeyState(Identifier identifier) {
        return dsl.select(EVENT.CURRENT_STATE).from(EVENT).join(CURRENT_KEY_STATE)
                  .on(EVENT.COORDINATES.eq(CURRENT_KEY_STATE.CURRENT)).join(IDENTIFIER)
                  .on(IDENTIFIER.PREFIX.eq(identifier.toIdent().toByteArray()))
                  .where(CURRENT_KEY_STATE.IDENTIFIER.eq(IDENTIFIER.ID)).fetchOptional().map(r -> {
                      try {
                          return new KeyStateImpl(decompress(r.value1()));
                      } catch (InvalidProtocolBufferException e) {
                          return null;
                      }
                  });
    }

}
