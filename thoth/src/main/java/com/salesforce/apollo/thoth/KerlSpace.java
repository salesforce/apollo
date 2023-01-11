/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import static com.salesforce.apollo.stereotomy.schema.tables.Coordinates.COORDINATES;
import static com.salesforce.apollo.stereotomy.schema.tables.Event.EVENT;
import static com.salesforce.apollo.stereotomy.schema.tables.Identifier.IDENTIFIER;
import static com.salesforce.apollo.thoth.schema.tables.IdentifierLocationHash.IDENTIFIER_LOCATION_HASH;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.h2.jdbcx.JdbcConnectionPool;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
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
import com.salesforce.apollo.stereotomy.event.KeyEvent;
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
        try (var connection = connectionPool.getConnection()) {
            var dsl = DSL.using(connection);
            dsl.transaction(ctx -> {
                var context = DSL.using(ctx);
                var insertEvent = context.batch(context.insertInto(EVENT)
                                                       .columns(EVENT.COORDINATES, EVENT.DIGEST, EVENT.CONTENT)
                                                       .values((Long) null, null, null));
                for (var event : events) {
                    event.getValidations();
                    event.getAttachment();
                    Long coordinates = null;
                    Digest digest = digestAlgorithm.digest(event.toByteString());
                    insertEvent.bind(coordinates, digest.toDigeste().toByteArray(), event.toByteArray());
                }
                insertEvent.execute();
            });
            dsl.fetchCount(dsl.selectFrom(IDENTIFIER));
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
        return dsl.select(EVENT.DIGEST)
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
                  .filter(d -> d != null);
    }
}
