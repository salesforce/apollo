/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import static com.salesforce.apollo.stereotomy.schema.tables.Attachment.ATTACHMENT;
import static com.salesforce.apollo.stereotomy.schema.tables.Coordinates.COORDINATES;
import static com.salesforce.apollo.stereotomy.schema.tables.Event.EVENT;
import static com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
import static com.salesforce.apollo.stereotomy.schema.tables.Receipt.RECEIPT;
import static com.salesforce.apollo.stereotomy.schema.tables.Validation.VALIDATION;
import static com.salesforce.apollo.thoth.schema.tables.IdentifierLocationHash.IDENTIFIER_LOCATION_HASH;
import static com.salesforce.apollo.thoth.schema.tables.PendingCoordinates.PENDING_COORDINATES;
import static com.salesforce.apollo.thoth.schema.tables.PendingEvent.PENDING_EVENT;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.h2.jdbcx.JdbcConnectionPool;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachmentAndValidations_;
import com.salesfoce.apollo.stereotomy.event.proto.Validation_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesfoce.apollo.thoth.proto.Intervals;
import com.salesfoce.apollo.thoth.proto.Update;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.DigestKERL;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.db.UniKERL;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;

/**
 * Represents the replicated KERL logic
 * 
 * @author hal.hildebrand
 *
 */
public class KerlSpace {
    private static final Logger log = LoggerFactory.getLogger(KerlSpace.class);

    public static void upsert(DSLContext dsl, EventCoords coordinates, Attachment attachment) {
        final var identBytes = coordinates.getIdentifier().toByteArray();

        var ident = dsl.newRecord(IDENTIFIER);
        ident.setPrefix(identBytes);
        ident.merge();

        Record1<Long> id;
        try {
            id = dsl.insertInto(PENDING_COORDINATES)
                    .set(PENDING_COORDINATES.DIGEST, coordinates.getDigest().toByteArray())
                    .set(PENDING_COORDINATES.IDENTIFIER,
                         dsl.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)))
                    .set(PENDING_COORDINATES.ILK, coordinates.getIlk())
                    .set(PENDING_COORDINATES.SEQUENCE_NUMBER,
                         ULong.valueOf(coordinates.getSequenceNumber()).toBigInteger())
                    .returningResult(PENDING_COORDINATES.ID)
                    .fetchOne();
        } catch (DataAccessException e) {
            // Already exists
            id = dsl.select(PENDING_COORDINATES.ID)
                    .from(PENDING_COORDINATES)
                    .join(IDENTIFIER)
                    .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toByteArray()))
                    .where(PENDING_COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                    .and(PENDING_COORDINATES.DIGEST.eq(coordinates.getDigest().toByteArray()))
                    .and(PENDING_COORDINATES.SEQUENCE_NUMBER.eq(ULong.valueOf(coordinates.getSequenceNumber())
                                                                     .toBigInteger()))
                    .and(PENDING_COORDINATES.ILK.eq(coordinates.getIlk()))
                    .fetchOne();
        }
        var count = new AtomicInteger();
        for (var s : attachment.getSealsList()) {
            final var bytes = s.toByteArray();
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
        log.info("appended: {} seals out of: {} coords: {}", count.get(), attachment.getSealsCount(), coordinates);

        count.set(0);
        for (var entry : attachment.getEndorsementsMap().entrySet()) {
            count.accumulateAndGet(dsl.mergeInto(RECEIPT)
                                      .usingDual()
                                      .on(RECEIPT.FOR.eq(id.value1()).and(RECEIPT.WITNESS.eq(entry.getKey())))
                                      .whenNotMatchedThenInsert()
                                      .set(RECEIPT.FOR, id.value1())
                                      .set(RECEIPT.WITNESS, entry.getKey())
                                      .set(RECEIPT.SIGNATURE, entry.getValue().toByteArray())
                                      .execute(),
                                   (a, b) -> a + b);
        }
        log.info("appended: {} endorsements out of: {} coords: {}", count.get(), attachment.getEndorsementsCount(),
                 coordinates);
    }

    public static void upsert(DSLContext context, KeyEvent event, DigestAlgorithm digestAlgorithm) {
        final EventCoordinates prevCoords = event.getPrevious();

        final var identBytes = event.getIdentifier().toIdent().toByteArray();

        context.mergeInto(IDENTIFIER)
               .using(context.selectOne())
               .on(IDENTIFIER.PREFIX.eq(identBytes))
               .whenNotMatchedThenInsert(IDENTIFIER.PREFIX)
               .values(identBytes)
               .execute();
        long id;
        try {
            id = context.insertInto(PENDING_COORDINATES)
                        .set(PENDING_COORDINATES.DIGEST, prevCoords.getDigest().toDigeste().toByteArray())
                        .set(PENDING_COORDINATES.IDENTIFIER,
                             context.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)))
                        .set(PENDING_COORDINATES.ILK, event.getIlk())
                        .set(PENDING_COORDINATES.SEQUENCE_NUMBER, event.getSequenceNumber().toBigInteger())
                        .returningResult(PENDING_COORDINATES.ID)
                        .fetchOne()
                        .value1();
        } catch (DataAccessException e) {
            // Already exists
            var coordinates = event.getCoordinates();
            id = context.select(PENDING_COORDINATES.ID)
                        .from(PENDING_COORDINATES)
                        .join(IDENTIFIER)
                        .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toIdent().toByteArray()))
                        .where(PENDING_COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                        .and(PENDING_COORDINATES.DIGEST.eq(coordinates.getDigest().toDigeste().toByteArray()))
                        .and(PENDING_COORDINATES.SEQUENCE_NUMBER.eq(coordinates.getSequenceNumber().toBigInteger()))
                        .and(PENDING_COORDINATES.ILK.eq(coordinates.getIlk()))
                        .fetchOne()
                        .value1();
        }

        final var digest = event.hash(digestAlgorithm);
        try {
            context.insertInto(PENDING_EVENT)
                   .set(PENDING_EVENT.COORDINATES, id)
                   .set(PENDING_EVENT.DIGEST, digest.toDigeste().toByteArray())
                   .set(PENDING_EVENT.CONTENT, UniKERL.compress(event.getBytes()))
                   .execute();
        } catch (DataAccessException e) {
            return;
        }
    }

    public static void upsertValidations(DSLContext dsl, Validations validations) {
        final var coordinates = validations.getCoordinates();
        final var identBytes = coordinates.getIdentifier().toByteArray();

        try {
            dsl.mergeInto(IDENTIFIER)
               .using(dsl.selectOne())
               .on(IDENTIFIER.PREFIX.eq(identBytes))
               .whenNotMatchedThenInsert(IDENTIFIER.PREFIX)
               .values(identBytes)
               .execute();
        } catch (DataAccessException e) {
            log.trace("Duplicate inserting identifier: {}", Identifier.from(coordinates.getIdentifier()));
        }

        Record1<Long> id;
        try {
            id = dsl.insertInto(COORDINATES)
                    .set(COORDINATES.DIGEST, coordinates.getDigest().toByteArray())
                    .set(COORDINATES.IDENTIFIER,
                         dsl.select(IDENTIFIER.ID).from(IDENTIFIER).where(IDENTIFIER.PREFIX.eq(identBytes)))
                    .set(COORDINATES.ILK, coordinates.getIlk())
                    .set(COORDINATES.SEQUENCE_NUMBER, ULong.valueOf(coordinates.getSequenceNumber()).toBigInteger())
                    .returningResult(COORDINATES.ID)
                    .fetchOne();
        } catch (DataAccessException e) {
            // Already exists
            id = dsl.select(COORDINATES.ID)
                    .from(COORDINATES)
                    .join(IDENTIFIER)
                    .on(IDENTIFIER.PREFIX.eq(coordinates.getIdentifier().toByteArray()))
                    .where(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                    .and(COORDINATES.DIGEST.eq(coordinates.getDigest().toByteArray()))
                    .and(COORDINATES.SEQUENCE_NUMBER.eq(ULong.valueOf(coordinates.getSequenceNumber()).toBigInteger()))
                    .and(COORDINATES.ILK.eq(coordinates.getIlk()))
                    .fetchOne();
        }
        var result = new AtomicInteger();
        var l = id.value1();
        validations.getValidationsList().forEach(v -> {
            var vRec = dsl.newRecord(VALIDATION);
            vRec.setFor(l);
            vRec.setValidator(v.getValidator().toByteArray());
            vRec.setSignature(v.getSignature().toByteArray());
            result.accumulateAndGet(vRec.merge(), (a, b) -> a + b);
        });
        log.info("Inserted validations: {} out of : {} for event: {}", result.get(), validations.getValidationsCount(),
                 EventCoordinates.from(coordinates));
    }

    private final JdbcConnectionPool connectionPool;

    public KerlSpace(JdbcConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

    /**
     * Answer the bloom filter encoding the key events contained within the combined
     * intervals
     * 
     * @param seed      - the seed for the bloom filter's hash generator
     * @param intervals - the combined intervals containing the identifier location
     *                  hashes.
     * @param fpr       - the false positive rate for the bloom filter
     * @return the bloom filter of Digests bounded by the identifier location hash
     *         intervals
     */
    public Biff populate(long seed, CombinedIntervals intervals, double fpr) {
        DigestBloomFilter bff = new DigestBloomFilter(seed, cardinality(), fpr);
        try (var connection = connectionPool.getConnection()) {
            var dsl = DSL.using(connection);
            eventDigestsIn(intervals, dsl).forEach(d -> bff.add(d));
        } catch (SQLException e) {
            log.error("Unable populate bloom filter, cannot acquire JDBC connection", e);
        }
        return bff.toBff();
    }

    /**
     * Reconcile the intervals for our partner
     * 
     * @param intervals - the relevant intervals of identifiers and the event
     *                  digests of these identifiers the partner already have
     * @param kerl
     * @return the Update.Builder of missing key events, based on the supplied
     *         intervals
     */
    public Update.Builder reconcile(Intervals intervals, DigestKERL kerl) {
        var biff = BloomFilter.from(intervals.getHave());
        var update = Update.newBuilder();
        try (var connection = connectionPool.getConnection()) {
            var dsl = DSL.using(connection);
            intervals.getIntervalsList()
                     .stream()
                     .map(i -> new KeyInterval(i))
                     .flatMap(i -> eventDigestsIn(i, dsl))
                     .filter(d -> !biff.contains(d))
                     .map(d -> event(d, dsl, kerl))
                     .filter(ke -> ke != null)
                     .forEach(ke -> update.addEvents(ke));
        } catch (SQLException e) {
            log.error("Unable to provide estimated cardinality, cannot acquire JDBC connection", e);
            throw new IllegalStateException("Unable to provide estimated cardinality, cannot acquire JDBC connection",
                                            e);
        }
        return update;
    }

    /**
     * Update the key events in this space
     * 
     * @param events
     */
    public void update(List<KeyEventWithAttachmentAndValidations_> events, DigestAlgorithm digestAlgorithm) {
        if (events.isEmpty()) {
            return;
        }
        try (var connection = connectionPool.getConnection()) {
            var dsl = DSL.using(connection);
            dsl.transaction(ctx -> {
                var context = DSL.using(ctx);
                for (var evente_ : events) {
                    final var event = ProtobufEventFactory.from(evente_.getEvent());
                    upsert(context, event, digestAlgorithm);
                    upsertValidations(context, evente_.getValidations());
                    evente_.getAttachment();
                    evente_.getEvent();
                }
                commitPending(context);
            });

        } catch (SQLException e) {
            log.error("Unable to update events, cannot acquire JDBC connection", e);
            throw new IllegalStateException("Unable to update events, cannot acquire JDBC connection", e);
        }
    }

    // the estimated cardinality of the number of key events
    private int cardinality() {
        try (var connection = connectionPool.getConnection()) {
            var dsl = DSL.using(connection);
            return dsl.fetchCount(dsl.selectFrom(IDENTIFIER));
        } catch (SQLException e) {
            log.error("Unable to provide estimated cardinality, cannot acquire JDBC connection", e);
            throw new IllegalStateException("Unable to provide estimated cardinality, cannot acquire JDBC connection",
                                            e);
        }
    }

    private void commitPending(DSLContext context) {

    }

    private KeyEventWithAttachmentAndValidations_ event(Digest d, DSLContext dsl, DigestKERL kerl) {
        final var builder = KeyEventWithAttachmentAndValidations_.newBuilder();
        KeyEvent event;
        try {
            event = kerl.getKeyEvent(d).get();
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            log.error("Unable to retrieve event for: {}", d, e);
            return null;
        }
        if (event == null) {
            return null;
        }
        EventCoordinates coordinates = event.getCoordinates();
        try {
            kerl.getAttachment(coordinates).thenApply(a -> builder.setAttachment(a.toAttachemente())).get();
            kerl.getValidations(coordinates)
                .thenApply(vs -> Validations.newBuilder()
                                            .addAllValidations(vs.entrySet()
                                                                 .stream()
                                                                 .map(e -> Validation_.newBuilder()
                                                                                      .setValidator(e.getKey()
                                                                                                     .toEventCoords())
                                                                                      .setSignature(e.getValue()
                                                                                                     .toSig())
                                                                                      .build())
                                                                 .toList())
                                            .build())
                .thenApply(v -> builder.setValidations(v))
                .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Should have never happened", e);
        }
        builder.setEvent(event.toKeyEvent_());
        return builder.build();
    }

    private Stream<Digest> eventDigestsIn(CombinedIntervals intervals, DSLContext dsl) {
        return intervals.intervals().flatMap(interval -> eventDigestsIn(interval, dsl));
    }

    private Stream<Digest> eventDigestsIn(KeyInterval interval, DSLContext dsl) {
        return Stream.concat(dsl.select(EVENT.DIGEST)
                                .from(EVENT)
                                .join(COORDINATES)
                                .on(EVENT.COORDINATES.eq(COORDINATES.ID))
                                .join(IDENTIFIER)
                                .on(COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                                .join(IDENTIFIER_LOCATION_HASH)
                                .on(IDENTIFIER.ID.eq(IDENTIFIER_LOCATION_HASH.IDENTIFIER))
                                .where(IDENTIFIER_LOCATION_HASH.DIGEST.ge(interval.getBegin().getBytes()))
                                .and(IDENTIFIER_LOCATION_HASH.DIGEST.le(interval.getBegin().getBytes()))
                                .stream()
                                .map(r -> {
                                    try {
                                        return Digest.from(Digeste.parseFrom(r.value1()));
                                    } catch (InvalidProtocolBufferException e) {
                                        return null;
                                    }
                                })
                                .filter(d -> d != null),
                             dsl.select(PENDING_EVENT.DIGEST)
                                .from(PENDING_EVENT)
                                .join(PENDING_COORDINATES)
                                .on(PENDING_EVENT.COORDINATES.eq(PENDING_COORDINATES.ID))
                                .join(IDENTIFIER)
                                .on(PENDING_COORDINATES.IDENTIFIER.eq(IDENTIFIER.ID))
                                .join(IDENTIFIER_LOCATION_HASH)
                                .on(IDENTIFIER.ID.eq(IDENTIFIER_LOCATION_HASH.IDENTIFIER))
                                .where(IDENTIFIER_LOCATION_HASH.DIGEST.ge(interval.getBegin().getBytes()))
                                .and(IDENTIFIER_LOCATION_HASH.DIGEST.le(interval.getBegin().getBytes()))
                                .stream()
                                .map(r -> {
                                    try {
                                        return Digest.from(Digeste.parseFrom(r.value1()));
                                    } catch (InvalidProtocolBufferException e) {
                                        return null;
                                    }
                                })
                                .filter(d -> d != null));
    }
}
