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

import java.sql.Connection;
import java.util.Base64;

import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

/**
 * @author hal.hildebrand
 *
 */
public class UniKERLDirect extends UniKERL {
    private static final Logger log               = LoggerFactory.getLogger(UniKERL.class);
    private static final byte[] DIGEST_NONE_BYTES = Digest.NONE.toDigeste().toByteArray();

    public UniKERLDirect(Connection connection, DigestAlgorithm digestAlgorithm) {
        super(connection, digestAlgorithm);
    }

    @Override
    public void append(AttachmentEvent event, KeyState newState) {
        // TODO Auto-generated method stub

    }

    public void initialize() {
        dsl.transaction(ctx -> {
            var context = DSL.using(ctx);
            context.insertInto(IDENTIFIER).set(IDENTIFIER.ID, 0L)
                   .set(IDENTIFIER.PREFIX, EventCoordinates.NONE.getIdentifier().toIdent().toByteArray()).execute();
            context.insertInto(COORDINATES).set(COORDINATES.ID, 0L)
                   .set(COORDINATES.DIGEST, EventCoordinates.NONE.getDigest().toDigeste().toByteArray())
                   .set(COORDINATES.IDENTIFIER, 0L)
                   .set(COORDINATES.SEQUENCE_NUMBER, EventCoordinates.NONE.getSequenceNumber())
                   .set(COORDINATES.ILK, EventCoordinates.NONE.getIlk()).execute();
            System.out.println(context.selectFrom(COORDINATES).fetch());
        });
    }

    @Override
    public void append(KeyEvent event, KeyState newState) {
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
                System.out.println(Base64.getUrlEncoder().withoutPadding()
                                         .encodeToString(prevCoords.getDigest().toDigeste().toByteArray()));

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
            System.out.println(context.selectFrom(COORDINATES).fetch());
            System.out.println(context.selectFrom(EVENT).fetch());
        });
    }
}
