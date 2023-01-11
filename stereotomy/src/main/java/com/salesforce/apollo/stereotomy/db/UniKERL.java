/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.toKeyEvent;
import static com.salesforce.apollo.stereotomy.schema.tables.Attachment.ATTACHMENT;
import static com.salesforce.apollo.stereotomy.schema.tables.Coordinates.COORDINATES;
import static com.salesforce.apollo.stereotomy.schema.tables.CurrentKeyState.CURRENT_KEY_STATE;
import static com.salesforce.apollo.stereotomy.schema.tables.Event.EVENT;
import static com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
import static com.salesforce.apollo.stereotomy.schema.tables.Receipt.RECEIPT;
import static com.salesforce.apollo.stereotomy.schema.tables.Validation.VALIDATION;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Sealed;
import com.salesfoce.apollo.utils.proto.Sig;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.DigestKERL;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.AttachmentImpl;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.protobuf.AttachmentEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.processing.KeyEventProcessor;

/**
 * @author hal.hildebrand
 *
 */
abstract public class UniKERL implements DigestKERL {
    private static final byte[] DIGEST_NONE_BYTES = Digest.NONE.toDigeste().toByteArray();
    private static final Logger log               = LoggerFactory.getLogger(UniKERL.class);

    public static void append(DSLContext dsl, AttachmentEvent attachment) {
        var coordinates = attachment.coordinates();
        final var identBytes = coordinates.getIdentifier().toIdent().toByteArray();

        var ident = dsl.newRecord(IDENTIFIER);
        ident.setPrefix(identBytes);
        ident.merge();

        Record1<Long> id;
        try {
            id = dsl.insertInto(COORDINATES)
                    .set(COORDINATES.DIGEST, coordinates.getDigest().getBytes())
                    .set(COORDINATES.IDENTIFIER,
                         dsl.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)))
                    .set(COORDINATES.ILK, coordinates.getIlk())
                    .set(COORDINATES.SEQUENCE_NUMBER, coordinates.getSequenceNumber().longValue())
                    .returningResult(COORDINATES.ID)
                    .fetchOne();
        } catch (DataAccessException e) {
            // Already exists
            id = dsl.select(COORDINATES.ID)
                    .from(COORDINATES)
                    .join(IDENTIFIER)
                    .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toIdent().toByteArray()))
                    .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                    .and(COORDINATES.DIGEST.eq(coordinates.getDigest().toDigeste().toByteArray()))
                    .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                    .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                    .fetchOne();
        }
        var count = new AtomicInteger();
        for (Seal s : attachment.attachments().seals()) {
            final var bytes = s.toSealed().toByteArray();
            count.accumulateAndGet(dsl.mergeInto(ATTACHMENT)
                                      .usingDual()
                                      .on(ATTACHMENT.FOR.eq(id.value1()))
                                      .and(ATTACHMENT.SEAL.eq(bytes))
                                      .whenNotMatchedThenInsert()
                                      .set(ATTACHMENT.FOR, id.value1())
                                      .set(ATTACHMENT.SEAL, bytes)
                                      .execute(),
                                   (a, b) -> a + b);
        }
        log.info("appended: {} seals out of: {} coords: {}", count.get(), attachment.attachments().seals().size(),
                 coordinates);

        count.set(0);
        for (var entry : attachment.attachments().endorsements().entrySet()) {
            count.accumulateAndGet(dsl.mergeInto(RECEIPT)
                                      .usingDual()
                                      .on(RECEIPT.FOR.eq(id.value1()).and(RECEIPT.WITNESS.eq(entry.getKey())))
                                      .whenNotMatchedThenInsert()
                                      .set(RECEIPT.FOR, id.value1())
                                      .set(RECEIPT.WITNESS, entry.getKey())
                                      .set(RECEIPT.SIGNATURE, entry.getValue().toSig().toByteArray())
                                      .execute(),
                                   (a, b) -> a + b);
        }
        log.info("appended: {} endorsements out of: {} coords: {}", count.get(),
                 attachment.attachments().endorsements().size(), coordinates);
    }

    public static void append(DSLContext context, KeyEvent event, KeyState newState, DigestAlgorithm digestAlgorithm) {
        final EventCoordinates prevCoords = event.getPrevious();
        final var preIdentifier = context.select(IDENTIFIER.ID)
                                         .from(IDENTIFIER)
                                         .where(IDENTIFIER.PREFIX.eq(prevCoords.getIdentifier()
                                                                               .toIdent()
                                                                               .toByteArray()));
        final var prev = context.select(COORDINATES.ID)
                                .from(COORDINATES)
                                .where(COORDINATES.DIGEST.eq(prevCoords.getDigest().toDigeste().toByteArray()))
                                .and(COORDINATES.IDENTIFIER.eq(preIdentifier))
                                .and(COORDINATES.SEQUENCE_NUMBER.eq(prevCoords.getSequenceNumber().longValue()))
                                .and(COORDINATES.ILK.eq(prevCoords.getIlk()))
                                .fetchOne();
        if (prev == null) {
            log.error("Cannot find previous coordinates: {}", prevCoords);
            throw new IllegalArgumentException("Cannot find previous coordinates: " + prevCoords
            + " for inserted event");
        }
        final var prevDigest = context.select(EVENT.DIGEST)
                                      .from(EVENT)
                                      .where(EVENT.COORDINATES.eq(prev.value1()))
                                      .fetchOne();

        final var identBytes = event.getIdentifier().toIdent().toByteArray();

        context.mergeInto(IDENTIFIER)
               .using(context.selectOne())
               .on(IDENTIFIER.PREFIX.eq(identBytes))
               .whenNotMatchedThenInsert(IDENTIFIER.PREFIX)
               .values(identBytes)
               .execute();

        var identifierId = context.select(IDENTIFIER.ID)
                                  .from(IDENTIFIER)
                                  .where(IDENTIFIER.PREFIX.eq(identBytes))
                                  .fetchOne();
        long id;
        try {
            id = context.insertInto(COORDINATES)
                        .set(COORDINATES.DIGEST, prevDigest == null ? DIGEST_NONE_BYTES : prevDigest.value1())
                        .set(COORDINATES.IDENTIFIER,
                             context.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)))
                        .set(COORDINATES.ILK, event.getIlk())
                        .set(COORDINATES.SEQUENCE_NUMBER, event.getSequenceNumber().longValue())
                        .returningResult(COORDINATES.ID)
                        .fetchOne()
                        .value1();
        } catch (DataAccessException e) {
            // Already exists
            var coordinates = event.getCoordinates();
            id = context.select(COORDINATES.ID)
                        .from(COORDINATES)
                        .join(IDENTIFIER)
                        .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toIdent().toByteArray()))
                        .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                        .and(COORDINATES.DIGEST.eq(coordinates.getDigest().toDigeste().toByteArray()))
                        .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                        .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                        .fetchOne()
                        .value1();
        }

        final var digest = event.hash(digestAlgorithm);
        try {
            context.insertInto(EVENT)
                   .set(EVENT.COORDINATES, id)
                   .set(EVENT.DIGEST, digest.toDigeste().toByteArray())
                   .set(EVENT.CONTENT, compress(event.getBytes()))
                   .set(EVENT.CURRENT_STATE, compress(newState.getBytes()))
                   .execute();
        } catch (DataAccessException e) {
            return;
        }
        log.trace("Inserted event: {}", event);
        context.mergeInto(CURRENT_KEY_STATE)
               .using(context.selectOne())
               .on(CURRENT_KEY_STATE.IDENTIFIER.eq(identifierId.value1()))
               .whenMatchedThenUpdate()
               .set(CURRENT_KEY_STATE.CURRENT, id)
               .whenNotMatchedThenInsert()
               .set(CURRENT_KEY_STATE.IDENTIFIER, identifierId.value1())
               .set(CURRENT_KEY_STATE.CURRENT, id)
               .execute();
    }

    public static void appendAttachments(Connection connection, List<byte[]> attachments) {
        attachments.forEach(bytes -> {
            AttachmentEvent event;
            try {
                event = new AttachmentEventImpl(com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent.parseFrom(bytes));
            } catch (InvalidProtocolBufferException e) {
                log.error("Error deserializing attachment event", e);
                return;
            }
            append(DSL.using(connection), event);
        });
    }

    public static byte[] appendEvent(Connection connection, byte[] event, String ilk, int digestCode) {
        final var uni = new UniKERLDirect(connection, DigestAlgorithm.fromDigestCode(digestCode));
        var result = uni.append(ProtobufEventFactory.toKeyEvent(event, ilk)).getNow(null);

        return result == null ? null : result.getBytes();
    }

    public static void appendValidations(DSLContext dsl, EventCoordinates coordinates,
                                         Map<EventCoordinates, JohnHancock> validations) {
        final var identBytes = coordinates.getIdentifier().toIdent().toByteArray();

        try {
            dsl.mergeInto(IDENTIFIER)
               .using(dsl.selectOne())
               .on(IDENTIFIER.PREFIX.eq(identBytes))
               .whenNotMatchedThenInsert(IDENTIFIER.PREFIX)
               .values(identBytes)
               .execute();
        } catch (DataAccessException e) {
            log.trace("Duplicate inserting identifier: {}", coordinates.getIdentifier());
        }

        Record1<Long> id;
        try {
            id = dsl.insertInto(COORDINATES)
                    .set(COORDINATES.DIGEST, coordinates.getDigest().getBytes())
                    .set(COORDINATES.IDENTIFIER,
                         dsl.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)))
                    .set(COORDINATES.ILK, coordinates.getIlk())
                    .set(COORDINATES.SEQUENCE_NUMBER, coordinates.getSequenceNumber().longValue())
                    .returningResult(COORDINATES.ID)
                    .fetchOne();
        } catch (DataAccessException e) {
            // Already exists
            id = dsl.select(COORDINATES.ID)
                    .from(COORDINATES)
                    .join(IDENTIFIER)
                    .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toIdent().toByteArray()))
                    .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                    .and(COORDINATES.DIGEST.eq(coordinates.getDigest().toDigeste().toByteArray()))
                    .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                    .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                    .fetchOne();
        }
        var result = new AtomicInteger();
        var l = id.value1();
        validations.forEach((coords, signature) -> {
            var vRec = dsl.newRecord(VALIDATION);
            vRec.setFor(l);
            vRec.setValidator(coords.toEventCoords().toByteArray());
            vRec.setSignature(signature.toSig().toByteArray());
            result.accumulateAndGet(vRec.merge(), (a, b) -> a + b);
        });
        log.info("Inserted validations: {} out of : {} for event: {}", result.get(), validations.size(), coordinates);
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

            context.mergeInto(IDENTIFIER)
                   .using(context.selectOne())
                   .on(IDENTIFIER.ID.eq(0L))
                   .whenNotMatchedThenInsert(IDENTIFIER.ID, IDENTIFIER.PREFIX)
                   .values(0L, ecNone.getIdentifier().toIdent().toByteArray())
                   .execute();

            context.mergeInto(EVENT)
                   .using(context.selectOne())
                   .on(EVENT.COORDINATES.eq(0L))
                   .whenNotMatchedThenInsert(EVENT.COORDINATES, EVENT.DIGEST, EVENT.CONTENT)
                   .values(0L, ecNone.getDigest().toDigeste().toByteArray(), compress(new byte[0]))
                   .execute();

            context.mergeInto(COORDINATES)
                   .using(context.selectOne())
                   .on(COORDINATES.ID.eq(0L))
                   .whenNotMatchedThenInsert(COORDINATES.ID, COORDINATES.DIGEST, COORDINATES.IDENTIFIER,
                                             COORDINATES.SEQUENCE_NUMBER, COORDINATES.ILK)
                   .values(0L, ecNone.getDigest().toDigeste().toByteArray(), 0L, ecNone.getSequenceNumber().longValue(),
                           ecNone.getIlk())
                   .execute();
        });
    }

    public static void initializeKERL(Connection connection) {
        initialize(DSL.using(connection));
    }

    protected final DigestAlgorithm   digestAlgorithm;
    protected final DSLContext        dsl;
    protected final KeyEventProcessor processor;

    public UniKERL(Connection connection, DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        this.dsl = DSL.using(connection);
        processor = new KeyEventProcessor(this);
    }

    @Override
    public CompletableFuture<Attachment> getAttachment(EventCoordinates coordinates) {
        var fs = new CompletableFuture<Attachment>();
        try {
            var resolved = dsl.select(COORDINATES.ID)
                              .from(COORDINATES)
                              .join(IDENTIFIER)
                              .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toIdent().toByteArray()))
                              .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                              .and(COORDINATES.DIGEST.eq(coordinates.getDigest().toDigeste().toByteArray()))
                              .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                              .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                              .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                              .fetchOne();
            if (resolved == null) {
                fs.complete(null);
                return fs;
            }

            var seals = dsl.select(ATTACHMENT.SEAL)
                           .from(ATTACHMENT)
                           .where(ATTACHMENT.FOR.eq(resolved.value1()))
                           .fetch()
                           .stream()
                           .map(r -> {
                               try {
                                   return Seal.from(Sealed.parseFrom(r.value1()));
                               } catch (InvalidProtocolBufferException e) {
                                   log.error("Error deserializing seal: {}", e);
                                   return null;
                               }
                           })
                           .filter(s -> s != null)
                           .toList();

            record receipt(int witness, Sig signature) {}
            var receipts = dsl.select(RECEIPT.WITNESS, RECEIPT.SIGNATURE)
                              .from(RECEIPT)
                              .where(RECEIPT.FOR.eq(resolved.value1()))
                              .fetch()
                              .stream()
                              .map(r -> {
                                  try {
                                      return new receipt(r.value1(), Sig.parseFrom(r.value2()));
                                  } catch (InvalidProtocolBufferException e) {
                                      log.error("Error deserializing signature witness: {}", e);
                                      return null;
                                  }
                              })
                              .filter(s -> s != null)
                              .collect(Collectors.toMap(r -> r.witness, r -> JohnHancock.from(r.signature)));
            fs.complete(new AttachmentImpl(seals, receipts));
        } catch (Throwable t) {
            fs.completeExceptionally(t);
        }
        return fs;
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    @Override
    public CompletableFuture<KeyEvent> getKeyEvent(Digest digest) {
        var fs = new CompletableFuture<KeyEvent>();
        try {
            var result = dsl.select(EVENT.CONTENT, COORDINATES.ILK)
                            .from(EVENT)
                            .join(COORDINATES)
                            .on(COORDINATES.ID.eq(EVENT.COORDINATES))
                            .where(EVENT.DIGEST.eq(digest.toDigeste().toByteString().toByteArray()))
                            .fetchOptional()
                            .map(r -> toKeyEvent(decompress(r.value1()), r.value2()))
                            .orElse(null);
            log.info("Get key event: {} result: {}", digest, result);
            fs.complete(result);
        } catch (Throwable t) {
            fs.completeExceptionally(t);
        }
        return fs;
    }

    @Override
    public CompletableFuture<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        var fs = new CompletableFuture<KeyEvent>();
        try {
            var result = dsl.select(EVENT.CONTENT, COORDINATES.ILK)
                            .from(EVENT)
                            .join(COORDINATES)
                            .on(EVENT.COORDINATES.eq(COORDINATES.ID))
                            .join(IDENTIFIER)
                            .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toIdent().toByteArray()))
                            .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                            .and(COORDINATES.DIGEST.eq(coordinates.getDigest().toDigeste().toByteArray()))
                            .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                            .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                            .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                            .fetchOptional()
                            .map(r -> toKeyEvent(decompress(r.value1()), r.value2()))
                            .orElse(null);
            log.info("Get key event: {} result: {}", coordinates, result);
            fs.complete(result);
        } catch (Throwable t) {
            log.error("Get key event: {} error", coordinates, t);
            fs.completeExceptionally(t);
        }
        return fs;
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(EventCoordinates coordinates) {
        var fs = new CompletableFuture<KeyState>();
        try {
            var result = dsl.select(EVENT.CURRENT_STATE)
                            .from(EVENT)
                            .join(COORDINATES)
                            .on(EVENT.COORDINATES.eq(COORDINATES.ID))
                            .join(IDENTIFIER)
                            .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toIdent().toByteArray()))
                            .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                            .and(COORDINATES.DIGEST.eq(coordinates.getDigest().toDigeste().toByteArray()))
                            .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                            .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                            .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                            .fetchOptional()
                            .map(r -> {
                                try {
                                    return new KeyStateImpl(decompress(r.value1()));
                                } catch (InvalidProtocolBufferException e) {
                                    log.warn("Cannot decode key state: {}", coordinates, e);
                                    return null;
                                }
                            })
                            .orElse(null);
            log.info("Get key state: {} result: {}", coordinates, result != null);
            fs.complete(result);
        } catch (Throwable t) {
            fs.completeExceptionally(t);
        }
        return fs;
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(Identifier identifier) {
        var fs = new CompletableFuture<KeyState>();
        try {
            var result = dsl.select(EVENT.CURRENT_STATE)
                            .from(EVENT)
                            .join(CURRENT_KEY_STATE)
                            .on(EVENT.COORDINATES.eq(CURRENT_KEY_STATE.CURRENT))
                            .join(IDENTIFIER)
                            .on(IDENTIFIER.PREFIX.eq(identifier.toIdent().toByteArray()))
                            .where(CURRENT_KEY_STATE.IDENTIFIER.eq(IDENTIFIER.ID))
                            .fetchOptional()
                            .map(r -> {
                                try {
                                    return new KeyStateImpl(decompress(r.value1()));
                                } catch (InvalidProtocolBufferException e) {
                                    log.warn("Cannot decode key state: {}", identifier, e);
                                    return null;
                                }
                            })
                            .orElse(null);
            log.info("Get key state: {} result: {}", identifier, result != null);
            fs.complete(result);
        } catch (Throwable t) {
            fs.completeExceptionally(t);
        }
        return fs;
    }

    @Override
    public CompletableFuture<Map<EventCoordinates, JohnHancock>> getValidations(EventCoordinates coordinates) {
        CompletableFuture<Map<EventCoordinates, JohnHancock>> complete = new CompletableFuture<>();
        var resolved = dsl.select(COORDINATES.ID)
                          .from(COORDINATES)
                          .join(IDENTIFIER)
                          .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toIdent().toByteArray()))
                          .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                          .and(COORDINATES.DIGEST.eq(coordinates.getDigest().toDigeste().toByteArray()))
                          .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                          .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                          .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().longValue()))
                          .fetchOne();
        if (resolved == null) {
            log.warn("Cannot resolve validations: {}", coordinates);
            complete.complete(Collections.emptyMap());
            return complete;
        }

        record validation(EventCoords coordinates, Sig signature) {}
        var validations = dsl.select(VALIDATION.VALIDATOR, VALIDATION.SIGNATURE)
                             .from(VALIDATION)
                             .where(VALIDATION.FOR.eq(resolved.value1()))
                             .fetch()
                             .stream()
                             .map(r -> {
                                 try {
                                     return new validation(EventCoords.parseFrom(r.value1()),
                                                           Sig.parseFrom(r.value2()));
                                 } catch (InvalidProtocolBufferException e) {
                                     log.error("Error deserializing signature witness: {}", e);
                                     return null;
                                 }
                             })
                             .filter(s -> s != null)
                             .collect(Collectors.toMap(v -> EventCoordinates.from(v.coordinates),
                                                       v -> JohnHancock.from(v.signature)));
        log.trace("Resolve validations: {} result: {}", coordinates, validations);
        complete.complete(validations);
        return complete;
    }
}
