/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.model.schema.tables.Coordinates.COORDINATES;
import static com.salesforce.apollo.model.schema.tables.Event.EVENT;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.sql.Connection;

import org.jooq.impl.DSL;

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
            context.insertInto(COORDINATES).set(COORDINATES.ID, 0L)
                   .set(COORDINATES.DIGEST, qb64(EventCoordinates.NONE.getDigest()))
                   .set(COORDINATES.IDENTIFIER, qb64(EventCoordinates.NONE.getIdentifier()))
                   .set(COORDINATES.SEQUENCE_NUMBER, EventCoordinates.NONE.getSequenceNumber())
                   .set(COORDINATES.ILK, EventCoordinates.NONE.getIlk()).execute();
        });
    }

    @Override
    public void append(KeyEvent event, KeyState newState) {
        dsl.transaction(ctx -> {
            var context = DSL.using(ctx);
            final var digest = qb64(event.hash(digestAlgorithm));
            final var identifier = qb64(event.getIdentifier());
            final var id = context.insertInto(COORDINATES).set(COORDINATES.DIGEST, digest)
                                  .set(COORDINATES.IDENTIFIER, identifier).set(COORDINATES.ILK, event.getIlk())
                                  .set(COORDINATES.SEQUENCE_NUMBER, event.getSequenceNumber())
                                  .returningResult(COORDINATES.ID).fetchOne().value1();
            final var prev = event.getPrevious();
            final Long prevId = context.select(COORDINATES.ID).from(COORDINATES)
                                       .where(COORDINATES.DIGEST.eq(qb64(prev.getDigest())))
                                       .and(COORDINATES.IDENTIFIER.eq(qb64(prev.getIdentifier())))
                                       .and(COORDINATES.SEQUENCE_NUMBER.eq(prev.getSequenceNumber()))
                                       .and(COORDINATES.ILK.eq(prev.getIlk())).fetchOne().value1();
            context.insertInto(EVENT).set(EVENT.COORDINATES, id).set(EVENT.CONTENT, event.getBytes())
                   .set(EVENT.PREVIOUS, prevId);
        });
    }
}
