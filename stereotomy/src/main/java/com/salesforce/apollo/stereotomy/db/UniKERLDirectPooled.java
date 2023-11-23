/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.stereotomy.db;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.DigestKERL;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KeyCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.h2.jdbcx.JdbcConnectionPool;
import org.joou.ULong;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * A version of the UniKERLDirect that uses a jdbc connection pool
 *
 * @author hal.hildebrand
 */
public class UniKERLDirectPooled {

    private final JdbcConnectionPool connectionPool;
    private final DigestAlgorithm    digestAlgorithm;

    public UniKERLDirectPooled(JdbcConnectionPool connectionPool, DigestAlgorithm digestAlgorithm) {
        this.connectionPool = connectionPool;
        this.digestAlgorithm = digestAlgorithm;
    }

    public ClosableKERL create() throws SQLException {
        return new ClosableKERL(connectionPool.getConnection());
    }

    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public class ClosableKERL implements Closeable, DigestKERL {
        private final Connection connection;
        private final DigestKERL kerl;

        public ClosableKERL(Connection connection) {
            this.connection = connection;
            this.kerl = new UniKERLDirect(connection, digestAlgorithm);
            try {
                connection.setAutoCommit(false);
            } catch (SQLException e) {
                throw new IllegalStateException("Cannot set auto commit to false", e);
            }
        }

        @Override
        public KeyState append(KeyEvent event) {
            return kerl.append(event);
        }

        @Override
        public List<KeyState> append(KeyEvent... event) {
            return kerl.append(event);
        }

        @Override
        public Void append(List<AttachmentEvent> event) {
            return kerl.append(event);
        }

        @Override
        public List<KeyState> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
            return kerl.append(events, attachments);
        }

        @Override
        public Void appendValidations(EventCoordinates coordinates, Map<EventCoordinates, JohnHancock> validations) {
            return kerl.appendValidations(coordinates, validations);
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                LoggerFactory.getLogger(ClosableKERL.class).error("Error closing connection", e);
            }
        }

        @Override
        public Attachment getAttachment(EventCoordinates coordinates) {
            return kerl.getAttachment(coordinates);
        }

        @Override
        public DigestAlgorithm getDigestAlgorithm() {
            return kerl.getDigestAlgorithm();
        }

        @Override
        public KeyEvent getKeyEvent(Digest digest) {
            return kerl.getKeyEvent(digest);
        }

        @Override
        public KeyEvent getKeyEvent(EventCoordinates coordinates) {
            return kerl.getKeyEvent(coordinates);
        }

        @Override
        public KeyState getKeyState(EventCoordinates coordinates) {
            return kerl.getKeyState(coordinates);
        }

        @Override
        public KeyState getKeyState(Identifier identifier) {
            return kerl.getKeyState(identifier);
        }

        @Override
        public KeyStateWithAttachments getKeyStateWithAttachments(EventCoordinates coordinates) {
            return kerl.getKeyStateWithAttachments(coordinates);
        }

        @Override
        public Map<EventCoordinates, JohnHancock> getValidations(EventCoordinates coordinates) {
            return kerl.getValidations(coordinates);
        }

        @Override
        public Verifier.DefaultVerifier getVerifier(KeyCoordinates coordinates) {
            return kerl.getVerifier(coordinates);
        }

        @Override
        public List<EventWithAttachments> kerl(Identifier identifier) {
            return kerl.kerl(identifier);
        }

        @Override
        public KeyState getKeyState(Identifier identifier, ULong sequenceNumber) {
            return kerl.getKeyState(identifier, sequenceNumber);
        }
    }
}
