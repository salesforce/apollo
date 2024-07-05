/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.proto.Sig;
import com.salesforce.apollo.stereotomy.DigestKERL;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.AttachmentImpl;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.event.proto.Sealed;
import com.salesforce.apollo.stereotomy.event.protobuf.AttachmentEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.processing.KeyEventProcessor;
import org.bouncycastle.mime.encoding.Base64InputStream;
import org.bouncycastle.mime.encoding.Base64OutputStream;
import org.h2.jdbc.JdbcSQLIntegrityConstraintViolationException;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.toKeyEvent;
import static com.salesforce.apollo.stereotomy.schema.tables.Attachment.ATTACHMENT;
import static com.salesforce.apollo.stereotomy.schema.tables.Coordinates.COORDINATES;
import static com.salesforce.apollo.stereotomy.schema.tables.CurrentKeyState.CURRENT_KEY_STATE;
import static com.salesforce.apollo.stereotomy.schema.tables.Event.EVENT;
import static com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
import static com.salesforce.apollo.stereotomy.schema.tables.Receipt.RECEIPT;
import static com.salesforce.apollo.stereotomy.schema.tables.Validation.VALIDATION;
import static com.salesforce.apollo.utils.Utils.b64;

/**
 * @author hal.hildebrand
 */
abstract public class UniKERL implements DigestKERL {
    public static final String DIGEST_NONE_ENCODED = b64(Digest.NONE.getBytes());

    private static final Logger            log = LoggerFactory.getLogger(UniKERL.class);
    protected final      DigestAlgorithm   digestAlgorithm;
    protected final      DSLContext        dsl;
    protected final      KeyEventProcessor processor;

    public UniKERL(Connection connection, DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        this.dsl = DSL.using(connection, SQLDialect.H2);
        processor = new KeyEventProcessor(this);
    }

    public static void append(DSLContext dsl, AttachmentEvent attachment) {
        if (attachment.attachments().seals().isEmpty() && attachment.attachments().endorsements().isEmpty()) {
            return;
        }
        var coordinates = attachment.coordinates();
        final var identBytes = b64(coordinates.getIdentifier().toIdent());

        var ident = dsl.newRecord(IDENTIFIER);
        ident.setPrefix(identBytes);
        ident.merge();

        Record1<Long> id;
        try {
            id = dsl.insertInto(COORDINATES)
                    .set(COORDINATES.DIGEST, b64(coordinates.getDigest().getBytes()))
                    .set(COORDINATES.IDENTIFIER,
                         dsl.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)))
                    .set(COORDINATES.ILK, coordinates.getIlk())
                    .set(COORDINATES.SEQUENCE_NUMBER, coordinates.getSequenceNumber().toBigInteger())
                    .returningResult(COORDINATES.ID)
                    .fetchOne();
        } catch (DataAccessException e) {
            // Already exists
            id = dsl.select(COORDINATES.ID)
                    .from(COORDINATES)
                    .join(IDENTIFIER)
                    .on(IDENTIFIER.PREFIX.eq(b64(coordinates.getIdentifier().toIdent())))
                    .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                    .and(COORDINATES.DIGEST.eq(b64(coordinates.getDigest().getBytes())))
                    .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                    .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                    .fetchOne();
        }
        var count = new AtomicInteger();
        for (Seal s : attachment.attachments().seals()) {
            final var bytes = s.toSealed().toByteArray();
            var seal = b64(bytes);
            count.accumulateAndGet(dsl.mergeInto(ATTACHMENT)
                                      .usingDual()
                                      .on(ATTACHMENT.FOR.eq(id.value1()))
                                      .and(ATTACHMENT.SEAL.eq(seal))
                                      .whenNotMatchedThenInsert()
                                      .set(ATTACHMENT.FOR, id.value1())
                                      .set(ATTACHMENT.SEAL, seal)
                                      .execute(), Integer::sum);
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
                                      .set(RECEIPT.SIGNATURE, b64(entry.getValue().toSig()))
                                      .execute(), Integer::sum);
        }
        log.info("appended: {} endorsements out of: {} coords: {}", count.get(),
                 attachment.attachments().endorsements().size(), coordinates);
    }

    public static void append(DSLContext context, KeyEvent event, KeyState newState, DigestAlgorithm digestAlgorithm) {
        assert newState != null;
        final EventCoordinates prevCoords = event.getPrevious();
        final var preIdentifier = context.select(IDENTIFIER.ID)
                                         .from(IDENTIFIER)
                                         .where(IDENTIFIER.PREFIX.eq(b64(prevCoords.getIdentifier().toIdent())));
        final var prev = context.select(COORDINATES.ID)
                                .from(COORDINATES)
                                .where(COORDINATES.DIGEST.eq(b64(prevCoords.getDigest().getBytes())))
                                .and(COORDINATES.IDENTIFIER.eq(preIdentifier))
                                .and(COORDINATES.SEQUENCE_NUMBER.eq(prevCoords.getSequenceNumber().toBigInteger()))
                                .and(COORDINATES.ILK.eq(prevCoords.getIlk()))
                                .fetchOne();
        if (prev == null) {
            log.error("Cannot find previous coordinates: {}", prevCoords);
            throw new IllegalArgumentException(
            "Cannot find previous coordinates: " + prevCoords + " for inserted event");
        }
        final var prevDigest = context.select(EVENT.DIGEST)
                                      .from(EVENT)
                                      .where(EVENT.COORDINATES.eq(prev.value1()))
                                      .fetchOne();

        final var identBytes = b64(event.getIdentifier().toIdent());
        try {
            context.mergeInto(IDENTIFIER)
                   .using(context.selectOne())
                   .on(IDENTIFIER.PREFIX.eq(identBytes))
                   .whenNotMatchedThenInsert(IDENTIFIER.PREFIX)
                   .values(identBytes)
                   .execute();
        } catch (DataAccessException e) {
            if (e.getCause() instanceof JdbcSQLIntegrityConstraintViolationException icv) {
                log.trace("Constraint violation ignored: {}", icv.toString());
            } else {
                throw e;
            }
        }

        var identifierId = context.select(IDENTIFIER.ID)
                                  .from(IDENTIFIER)
                                  .where(IDENTIFIER.PREFIX.eq(identBytes))
                                  .fetchOne();
        long id;
        try {
            id = context.insertInto(COORDINATES)
                        .set(COORDINATES.DIGEST, prevDigest == null ? DIGEST_NONE_ENCODED : prevDigest.value1())
                        .set(COORDINATES.IDENTIFIER,
                             context.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)))
                        .set(COORDINATES.ILK, event.getIlk())
                        .set(COORDINATES.SEQUENCE_NUMBER, event.getSequenceNumber().toBigInteger())
                        .returningResult(COORDINATES.ID)
                        .fetchOne()
                        .value1();
        } catch (DataAccessException e) {
            log.trace("already published: {} : {}", event.getCoordinates(), e.toString());
            // Already exists
            var coordinates = event.getCoordinates();
            id = context.select(COORDINATES.ID)
                        .from(COORDINATES)
                        .join(IDENTIFIER)
                        .on(IDENTIFIER.PREFIX.eq(b64(coordinates.getIdentifier().toIdent())))
                        .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                        .and(COORDINATES.DIGEST.eq(b64(coordinates.getDigest().getBytes())))
                        .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                        .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                        .fetchOne()
                        .value1();
        }

        final var digest = event.hash(digestAlgorithm);
        try {
            context.insertInto(EVENT)
                   .set(EVENT.COORDINATES, id)
                   .set(EVENT.DIGEST, b64(digest.getBytes()))
                   .set(EVENT.CONTENT, compress(event.getBytes()))
                   .set(EVENT.CURRENT_STATE, compress(newState.getBytes()))
                   .execute();
        } catch (DataAccessException e) {
            // ignore
            log.trace("already inserted event: {} : {}", e, e.toString());
        }
        context.mergeInto(CURRENT_KEY_STATE)
               .using(context.selectOne())
               .on(CURRENT_KEY_STATE.IDENTIFIER.eq(identifierId.value1()))
               .whenMatchedThenUpdate()
               .set(CURRENT_KEY_STATE.CURRENT, id)
               .whenNotMatchedThenInsert()
               .set(CURRENT_KEY_STATE.IDENTIFIER, identifierId.value1())
               .set(CURRENT_KEY_STATE.CURRENT, id)
               .execute();
        log.info("Inserted key state: {}", event);
    }

    public static void appendAttachments(Connection connection, List<byte[]> attachments) {
        attachments.forEach(bytes -> {
            AttachmentEvent event;
            try {
                event = new AttachmentEventImpl(
                com.salesforce.apollo.stereotomy.event.proto.AttachmentEvent.parseFrom(bytes));
            } catch (InvalidProtocolBufferException e) {
                log.error("Error deserializing attachment event", e);
                return;
            }
            append(DSL.using(connection, SQLDialect.H2), event);
        });
    }

    public static byte[] appendEvent(Connection connection, byte[] event, String ilk, int digestCode) {
        final var uni = new UniKERLDirect(connection, DigestAlgorithm.fromDigestCode(digestCode));
        var result = uni.append(ProtobufEventFactory.toKeyEvent(event, ilk));

        return result == null ? null : result.getBytes();
    }

    public static void appendValidations(DSLContext dsl, EventCoordinates coordinates,
                                         Map<EventCoordinates, JohnHancock> validations) {
        if (validations.isEmpty()) {
            return;
        }
        final var identBytes = b64(coordinates.getIdentifier().toIdent());
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
                    .set(COORDINATES.DIGEST, b64(coordinates.getDigest().getBytes()))
                    .set(COORDINATES.IDENTIFIER,
                         dsl.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)))
                    .set(COORDINATES.ILK, coordinates.getIlk())
                    .set(COORDINATES.SEQUENCE_NUMBER, coordinates.getSequenceNumber().toBigInteger())
                    .returningResult(COORDINATES.ID)
                    .fetchOne();
        } catch (DataAccessException e) {
            // Already exists
            id = dsl.select(COORDINATES.ID)
                    .from(COORDINATES)
                    .join(IDENTIFIER)
                    .on(IDENTIFIER.PREFIX.eq(b64(coordinates.getIdentifier().toIdent())))
                    .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                    .and(COORDINATES.DIGEST.eq(b64(coordinates.getDigest().getBytes())))
                    .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                    .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                    .fetchOne();
            if (id == null) {
                log.info("Not found!: {}", coordinates.getIdentifier());
            }
        }
        if (id == null) {
            log.info("Not inserted!: {}", coordinates.getIdentifier());
        }
        var result = new AtomicInteger();
        var l = id.value1();
        validations.forEach((coords, signature) -> {
            var vRec = dsl.newRecord(VALIDATION);
            vRec.setFor(l);
            vRec.setValidator(b64(coords.toEventCoords()));
            vRec.setSignature(b64(signature.toSig()));
            result.accumulateAndGet(vRec.merge(), Integer::sum);
        });
        log.info("Inserted validations: {} out of : {} for event: {}", result.get(), validations.size(), coordinates);
    }

    public static String compress(byte[] input) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (var out = new Base64OutputStream(baos);
             GZIPOutputStream gzos = new GZIPOutputStream(out);
             ByteArrayInputStream bais = new ByteArrayInputStream(input)) {
            bais.transferTo(gzos);
            gzos.finish();
            gzos.flush();
            baos.flush();
            out.flush();
            baos.flush();
        } catch (IOException e) {
            throw new IllegalStateException("unable to compress input bytes", e);
        }
        return baos.toString();
    }

    public static byte[] decompress(String input) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (Base64InputStream b64 = new Base64InputStream(
        new ByteArrayInputStream(input.getBytes(Charset.defaultCharset())));
             GZIPInputStream gis = new GZIPInputStream(b64)) {
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
                   .values(0L, b64(ecNone.getIdentifier().toIdent()))
                   .execute();

            var bNone = b64(ecNone.getDigest().getBytes());
            context.mergeInto(EVENT)
                   .using(context.selectOne())
                   .on(EVENT.COORDINATES.eq(0L))
                   .whenNotMatchedThenInsert(EVENT.COORDINATES, EVENT.DIGEST, EVENT.CONTENT)
                   .values(0L, bNone, compress(new byte[0]))
                   .execute();

            context.mergeInto(COORDINATES)
                   .using(context.selectOne())
                   .on(COORDINATES.ID.eq(0L))
                   .whenNotMatchedThenInsert(COORDINATES.ID, COORDINATES.DIGEST, COORDINATES.IDENTIFIER,
                                             COORDINATES.SEQUENCE_NUMBER, COORDINATES.ILK)
                   .values(0L, bNone, 0L, ecNone.getSequenceNumber().toBigInteger(), ecNone.getIlk())
                   .execute();
        });
    }

    public static void initializeKERL(Connection connection) {
        initialize(DSL.using(connection, SQLDialect.H2));
    }

    @Override
    public Attachment getAttachment(EventCoordinates coordinates) {
        var resolved = dsl.select(COORDINATES.ID)
                          .from(COORDINATES)
                          .join(IDENTIFIER)
                          .on(IDENTIFIER.PREFIX.eq(b64(coordinates.getIdentifier().toIdent())))
                          .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                          .and(COORDINATES.DIGEST.eq(b64(coordinates.getDigest().getBytes())))
                          .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                          .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                          .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                          .fetchOne();
        if (resolved == null) {
            return null;
        }

        var seals = dsl.select(ATTACHMENT.SEAL)
                       .from(ATTACHMENT)
                       .where(ATTACHMENT.FOR.eq(resolved.value1()))
                       .fetch()
                       .stream()
                       .map(r -> {
                           try {
                               return Seal.from(Sealed.parseFrom(decompress(r.value1())));
                           } catch (InvalidProtocolBufferException e) {
                               log.error("Error deserializing seal: {}", e);
                               return null;
                           }
                       })
                       .filter(Objects::nonNull)
                       .toList();

        record receipt(int witness, Sig signature) {
        }
        var receipts = dsl.select(RECEIPT.WITNESS, RECEIPT.SIGNATURE)
                          .from(RECEIPT)
                          .where(RECEIPT.FOR.eq(resolved.value1()))
                          .fetch()
                          .stream()
                          .map(r -> {
                              try {
                                  return new receipt(r.value1(), Sig.parseFrom(decompress(r.value2())));
                              } catch (InvalidProtocolBufferException e) {
                                  log.error("Error deserializing signature witness: {}", e);
                                  return null;
                              }
                          })
                          .filter(Objects::nonNull)
                          .collect(Collectors.toMap(r -> r.witness, r -> JohnHancock.from(r.signature)));
        return new AttachmentImpl(seals, receipts);
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    @Override
    public KeyEvent getKeyEvent(Digest digest) {
        var result = dsl.select(EVENT.CONTENT, COORDINATES.ILK)
                        .from(EVENT)
                        .join(COORDINATES)
                        .on(COORDINATES.ID.eq(EVENT.COORDINATES))
                        .where(EVENT.DIGEST.eq(b64(digest.getBytes())))
                        .fetchOptional()
                        .map(r -> toKeyEvent(decompress(r.value1()), r.value2()))
                        .orElse(null);
        log.info("Get key event: {} result: {}", digest, result);
        return result;
    }

    @Override
    public KeyEvent getKeyEvent(EventCoordinates coordinates) {
        var result = dsl.select(EVENT.CONTENT, COORDINATES.ILK)
                        .from(EVENT)
                        .join(COORDINATES)
                        .on(EVENT.COORDINATES.eq(COORDINATES.ID))
                        .join(IDENTIFIER)
                        .on(IDENTIFIER.PREFIX.eq(b64(coordinates.getIdentifier().toIdent())))
                        .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                        .and(COORDINATES.DIGEST.eq(b64(coordinates.getDigest().getBytes())))
                        .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                        .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                        .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                        .fetchOptional()
                        .map(r -> toKeyEvent(decompress(r.value1()), r.value2()))
                        .orElse(null);
        log.info("Get key event: {} result: {}", coordinates, result);
        return result;
    }

    @Override
    public KeyState getKeyState(EventCoordinates coordinates) {
        var result = dsl.select(EVENT.CURRENT_STATE)
                        .from(EVENT)
                        .join(COORDINATES)
                        .on(EVENT.COORDINATES.eq(COORDINATES.ID))
                        .join(IDENTIFIER)
                        .on(IDENTIFIER.PREFIX.eq(b64(coordinates.getIdentifier().toIdent())))
                        .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                        .and(COORDINATES.DIGEST.eq(b64(coordinates.getDigest().getBytes())))
                        .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                        .and(COORDINATES.ILK.eq(coordinates.getIlk()))
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
        log.info("Get key state coordinates: {} result: {}", coordinates, result != null);
        return result;
    }

    @Override
    public KeyState getKeyState(Identifier identifier, ULong sequenceNumber) {
        var result = dsl.select(EVENT.CURRENT_STATE)
                        .from(EVENT)
                        .join(COORDINATES)
                        .on(EVENT.COORDINATES.eq(COORDINATES.ID))
                        .join(IDENTIFIER)
                        .on(IDENTIFIER.PREFIX.eq(b64(identifier.toIdent())))
                        .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                        .and(COORDINATES.SEQUENCE_NUMBER.eq(sequenceNumber.toBigInteger()))
                        .fetchOptional()
                        .map(r -> {
                            try {
                                return new KeyStateImpl(decompress(r.value1()));
                            } catch (InvalidProtocolBufferException e) {
                                log.warn("Cannot decode key state for: {} sequence number: {}", identifier,
                                         sequenceNumber, e);
                                return null;
                            }
                        })
                        .orElse(null);
        log.info("Get key state for: {} sequence number: {} result: {}", identifier, sequenceNumber, result != null);
        return result;
    }

    @Override
    public KeyState getKeyState(Identifier identifier) {
        var result = dsl.select(EVENT.CURRENT_STATE)
                        .from(EVENT)
                        .join(CURRENT_KEY_STATE)
                        .on(EVENT.COORDINATES.eq(CURRENT_KEY_STATE.CURRENT))
                        .join(IDENTIFIER)
                        .on(IDENTIFIER.PREFIX.eq(b64(identifier.toIdent())))
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
        return result;
    }

    @Override
    public Map<EventCoordinates, JohnHancock> getValidations(EventCoordinates coordinates) {
        var resolved = dsl.select(COORDINATES.ID)
                          .from(COORDINATES)
                          .join(IDENTIFIER)
                          .on(IDENTIFIER.PREFIX.eq(b64(coordinates.getIdentifier().toIdent())))
                          .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                          .and(COORDINATES.DIGEST.eq(b64(coordinates.getDigest().getBytes())))
                          .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                          .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                          .and(COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                          .fetchOne();
        if (resolved == null) {
            log.warn("Cannot resolve validations: {}", coordinates);
            return Collections.emptyMap();
        }

        record validation(EventCoords coordinates, Sig signature) {
        }
        var validations = dsl.select(VALIDATION.VALIDATOR, VALIDATION.SIGNATURE)
                             .from(VALIDATION)
                             .where(VALIDATION.FOR.eq(resolved.value1()))
                             .fetch()
                             .stream()
                             .map(r -> {
                                 try {
                                     return new validation(EventCoords.parseFrom(b64(r.value1())),
                                                           Sig.parseFrom(b64(r.value2())));
                                 } catch (InvalidProtocolBufferException e) {
                                     log.error("Error deserializing signature witness: {}", e);
                                     return null;
                                 }
                             })
                             .filter(Objects::nonNull)
                             .collect(Collectors.toMap(v -> EventCoordinates.from(v.coordinates),
                                                       v -> JohnHancock.from(v.signature)));
        log.trace("Resolve validations: {} result: {}", coordinates, validations);
        return validations;
    }
}
