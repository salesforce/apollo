/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesfoce.apollo.stereotomy.event.proto.*;
import com.salesfoce.apollo.thoth.proto.Intervals;
import com.salesfoce.apollo.thoth.proto.Update;
import com.salesfoce.apollo.utils.proto.Biff;
import com.salesfoce.apollo.utils.proto.Digeste;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.DigestKERL;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.AttachmentEventImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter;
import com.salesforce.apollo.utils.bloomFilters.BloomFilter.DigestBloomFilter;
import org.h2.jdbcx.JdbcConnectionPool;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.salesforce.apollo.stereotomy.schema.tables.Coordinates.COORDINATES;
import static com.salesforce.apollo.stereotomy.schema.tables.Event.EVENT;
import static com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
import static com.salesforce.apollo.thoth.schema.tables.IdentifierLocationHash.IDENTIFIER_LOCATION_HASH;
import static com.salesforce.apollo.thoth.schema.tables.PendingAttachment.PENDING_ATTACHMENT;
import static com.salesforce.apollo.thoth.schema.tables.PendingCoordinates.PENDING_COORDINATES;
import static com.salesforce.apollo.thoth.schema.tables.PendingEvent.PENDING_EVENT;
import static com.salesforce.apollo.thoth.schema.tables.PendingValidations.PENDING_VALIDATIONS;

/**
 * Represents the replicated KERL logic
 *
 * @author hal.hildebrand
 */
public class KerlSpace {
    private static final Logger log = LoggerFactory.getLogger(KerlSpace.class);
    private final JdbcConnectionPool connectionPool;

    public KerlSpace(JdbcConnectionPool connectionPool) {
        this.connectionPool = connectionPool;
    }

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
        var vRec = dsl.newRecord(PENDING_ATTACHMENT);
        vRec.setCoordinates(id.value1());
        vRec.setAttachment(attachment.toByteArray());
        vRec.insert();
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
                    .set(PENDING_EVENT.EVENT, event.getBytes())
                    .execute();
        } catch (DataAccessException e) {
            return;
        }
    }

    public static void upsert(DSLContext dsl, Validations validations) {
        final var coordinates = validations.getCoordinates();
        final var logCoords = EventCoordinates.from(coordinates);
        final var logIdentifier = Identifier.from(coordinates.getIdentifier());
        log.trace("Upserting validations for: {}", logCoords);
        final var identBytes = coordinates.getIdentifier().toByteArray();

        try {
            dsl.mergeInto(IDENTIFIER)
                    .using(dsl.selectOne())
                    .on(IDENTIFIER.PREFIX.eq(identBytes))
                    .whenNotMatchedThenInsert(IDENTIFIER.PREFIX)
                    .values(identBytes)
                    .execute();
        } catch (DataAccessException e) {
            log.trace("Duplicate inserting identifier: {}", logIdentifier);
        }

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
            log.trace("Id: {} for: {}", id, logCoords);
        } catch (DataAccessException e) {
            log.trace("access exception for: {}", logCoords, e);
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
        if (id == null) {
            log.trace("Null coordinates ID for: {}", coordinates);
            return;
        }
        var vRec = dsl.newRecord(PENDING_VALIDATIONS);
        vRec.setCoordinates(id.value1());
        vRec.setValidations(validations.toByteArray());
        vRec.insert();
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
     * intervals
     */
    public Biff populate(long seed, CombinedIntervals intervals, double fpr) {
        DigestBloomFilter bff = new DigestBloomFilter(seed, cardinality(), fpr);
        try (var connection = connectionPool.getConnection()) {
            var dsl = DSL.using(connection);
            eventDigestsIn(intervals, dsl).forEach(d -> {
                bff.add(d);
            });
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
     * intervals
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
                    .forEach(ke -> {
                        update.addEvents(ke);
                    });
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
     * @param kerl
     */
    public void update(List<KeyEventWithAttachmentAndValidations_> events, KERL kerl) {
        if (events.isEmpty()) {
            return;
        }

        final var digestAlgorithm = kerl.getDigestAlgorithm();

        try (var connection = connectionPool.getConnection()) {
            var dsl = DSL.using(connection);
            dsl.transaction(ctx -> {
                var context = DSL.using(ctx);
                for (var evente_ : events) {
                    final var event = ProtobufEventFactory.from(evente_.getEvent());
                    if (!evente_.getValidations().equals(Validations.getDefaultInstance())) {
                        upsert(context, evente_.getValidations());
                    }
                    if (evente_.hasAttachment()) {
                        upsert(context, event.getCoordinates().toEventCoords(), evente_.getAttachment());
                    }
                    upsert(context, event, digestAlgorithm);
                }
            });
            commitPending(dsl, kerl);
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

    private void commitPending(DSLContext context, KERL kerl) {
        context.select(PENDING_COORDINATES.ID, PENDING_EVENT.EVENT, PENDING_COORDINATES.ILK)
                .from(PENDING_EVENT)
                .join(PENDING_COORDINATES)
                .on(PENDING_COORDINATES.ID.eq(PENDING_EVENT.COORDINATES))
                .join(EVENT)
                .on(EVENT.DIGEST.eq(PENDING_COORDINATES.DIGEST))
                .orderBy(PENDING_COORDINATES.SEQUENCE_NUMBER)
                .fetchStream()
                .forEach(r -> {
                    KeyEvent event = ProtobufEventFactory.toKeyEvent(r.value2(), r.value3());
                    EventCoordinates coordinates = event.getCoordinates();
                    if (coordinates != null) {
                        context.select(PENDING_ATTACHMENT.ATTACHMENT)
                                .from(PENDING_ATTACHMENT)
                                .where(PENDING_ATTACHMENT.COORDINATES.eq(r.value1()))
                                .stream()
                                .forEach(bytes -> {
                                    try {
                                        Attachment attach = Attachment.parseFrom(bytes.value1());
                                        kerl.append(Collections.singletonList(new AttachmentEventImpl(AttachmentEvent.newBuilder()
                                                .setCoordinates(coordinates.toEventCoords())
                                                .setAttachment(attach)
                                                .build())));
                                    } catch (InvalidProtocolBufferException e) {
                                        log.error("Cannot deserialize attachment", e);
                                    }
                                });
                        context.select(PENDING_VALIDATIONS.VALIDATIONS)
                                .from(PENDING_VALIDATIONS)
                                .where(PENDING_VALIDATIONS.COORDINATES.eq(r.value1()))
                                .stream()
                                .forEach(bytes -> {
                                    try {
                                        Validations attach = Validations.parseFrom(bytes.value1());
                                        kerl.appendValidations(coordinates,
                                                attach.getValidationsList()
                                                        .stream()
                                                        .collect(Collectors.toMap(v -> EventCoordinates.from(v.getValidator()),
                                                                v -> JohnHancock.from(v.getSignature()))));
                                    } catch (InvalidProtocolBufferException e) {
                                        log.error("Cannot deserialize validation", e);
                                    }
                                });
                        kerl.append(event);
                    }
                    context.deleteFrom(PENDING_COORDINATES).where(PENDING_COORDINATES.ID.eq(r.value1())).execute();
                });
    }

    private KeyEventWithAttachmentAndValidations_ event(Digest d, DSLContext dsl, DigestKERL kerl) {
        final var builder = KeyEventWithAttachmentAndValidations_.newBuilder();
        KeyEvent event = kerl.getKeyEvent(d);
        if (event == null) {
            return null;
        }
        EventCoordinates coordinates = event.getCoordinates();
        com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment a = kerl.getAttachment(coordinates);
        builder.setAttachment(a.toAttachemente());
        Map<EventCoordinates, JohnHancock> vs = kerl.getValidations(coordinates);
        var v = Validations.newBuilder()
                .setCoordinates(coordinates.toEventCoords())
                .addAllValidations(vs.entrySet()
                        .stream()
                        .map(e -> Validation_.newBuilder()
                                .setValidator(e.getKey()
                                        .toEventCoords())
                                .setSignature(e.getValue()
                                        .toSig())
                                .build())
                        .toList())
                .build();
        builder.setValidations(v);
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
                        .and(IDENTIFIER_LOCATION_HASH.DIGEST.le(interval.getEnd().getBytes()))
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
                        .and(IDENTIFIER_LOCATION_HASH.DIGEST.le(interval.getEnd().getBytes()))
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
