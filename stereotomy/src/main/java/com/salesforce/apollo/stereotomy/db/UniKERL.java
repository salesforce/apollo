/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.db;

import static com.salesforce.apollo.model.schema.tables.Event.*;
import static com.salesforce.apollo.model.schema.tables.Coordinates.*;
import static com.salesforce.apollo.crypto.QualifiedBase64.*;

import java.sql.Connection;
import java.util.Optional;
import java.util.OptionalLong;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import com.google.protobuf.InvalidProtocolBufferException;
import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.model.schema.tables.Coordinates;
import com.salesforce.apollo.model.schema.tables.Event;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.DelegatingEventCoordinates;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.SealingEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.mvlog.KeyStateImpl;

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
        return dsl.select(EVENT.CONTENT).from(EVENT)
                  .where(EVENT.COORDINATES.eq(dsl.select(COORDINATES.ID).from(COORDINATES)
                                                 .where(COORDINATES.DIGEST.eq(qb64(digest)))))
                  .fetchOptional().map(b -> mapKeyEvent(b)).map(e -> (KeyEvent) e);
    }

    private KeyStateImpl mapKeyEvent(Record1<byte[]> b) {
        try {
            return new KeyStateImpl(com.salesfoce.apollo.stereotomy.event.proto.KeyState.parseFrom(b.value1()));
        } catch (InvalidProtocolBufferException e) {
            return null;
        }
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Optional<String> getKeyEventHash(EventCoordinates coordinates) {
        // TODO Auto-generated method stub
        return null;
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
