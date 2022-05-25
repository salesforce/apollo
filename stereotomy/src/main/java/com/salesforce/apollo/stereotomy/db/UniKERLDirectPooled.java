/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.stereotomy.db;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.h2.jdbcx.JdbcConnectionPool;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * A version of the UniKERLDirect that uses a jdbc connection pool
 * 
 * @author hal.hildebrand
 *
 */
public class UniKERLDirectPooled {

    public class ClosableKERL implements Closeable, KERL {
        private final Connection connection;
        private final KERL       kerl;

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
        public CompletableFuture<Void> append(AttachmentEvent event) {
            return kerl.append(event);
        }

        @Override
        public CompletableFuture<KeyState> append(KeyEvent event) {
            return kerl.append(event);
        }

        @Override
        public CompletableFuture<List<KeyState>> append(KeyEvent... event) {
            return kerl.append(event);
        }

        @Override
        public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
            return kerl.append(events, attachments);
        }

        @Override
        public void close() throws IOException {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException("Error closing connection", e);
            }
        }

        @Override
        public CompletableFuture<Attachment> getAttachment(EventCoordinates coordinates) {
            return kerl.getAttachment(coordinates);
        }

        @Override
        public DigestAlgorithm getDigestAlgorithm() {
            return kerl.getDigestAlgorithm();
        }

        @Override
        public Optional<KeyEvent> getKeyEvent(Digest digest) {
            return kerl.getKeyEvent(digest);
        }

        @Override
        public Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
            return kerl.getKeyEvent(coordinates);
        }

        @Override
        public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
            return kerl.getKeyState(coordinates);
        }

        @Override
        public Optional<KeyState> getKeyState(Identifier identifier) {
            return kerl.getKeyState(identifier);
        }

        @Override
        public Optional<Verifier> getVerifier(KeyCoordinates coordinates) {
            return kerl.getVerifier(coordinates);
        }

        @Override
        public Optional<List<EventWithAttachments>> kerl(Identifier identifier) {
            return kerl.kerl(identifier);
        }
    }

    private final JdbcConnectionPool connectionPool;
    private final DigestAlgorithm    digestAlgorithm;

    public UniKERLDirectPooled(JdbcConnectionPool connectionPool, DigestAlgorithm digestAlgorithm) {
        this.connectionPool = connectionPool;
        this.digestAlgorithm = digestAlgorithm;
    }

    public ClosableKERL create() throws SQLException {
        Connection connection = connectionPool.getConnection();
        return new ClosableKERL(connection);
    }

    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }
}
