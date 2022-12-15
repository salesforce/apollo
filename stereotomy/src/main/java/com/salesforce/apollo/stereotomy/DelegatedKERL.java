/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.KeyStateWithEndorsementsAndValidations;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class DelegatedKERL implements KERL {
    protected final KERL delegate;

    public DelegatedKERL(KERL delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        return delegate.append(event);
    }

    @Override
    public CompletableFuture<List<KeyState>> append(KeyEvent... events) {
        return delegate.append(events);
    }

    @Override
    public CompletableFuture<Void> append(List<AttachmentEvent> events) {
        return delegate.append(events);
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        return delegate.append(events, attachments);
    }

    @Override
    public CompletableFuture<Void> appendValidations(EventCoordinates coordinates,
                                                     Map<EventCoordinates, JohnHancock> validations) {
        return delegate.appendValidations(coordinates, validations);
    }

    @Override
    public CompletableFuture<Attachment> getAttachment(EventCoordinates coordinates) {
        return delegate.getAttachment(coordinates);
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return delegate.getDigestAlgorithm();
    }

    @Override
    public CompletableFuture<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return delegate.getKeyEvent(coordinates);
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(EventCoordinates coordinates) {
        return delegate.getKeyState(coordinates);
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(Identifier identifier) {
        return delegate.getKeyState(identifier);
    }

    @Override
    public CompletableFuture<KeyStateWithAttachments> getKeyStateWithAttachments(EventCoordinates coordinates) {
        return delegate.getKeyStateWithAttachments(coordinates);
    }

    @Override
    public CompletableFuture<KeyStateWithEndorsementsAndValidations> getKeyStateWithEndorsementsAndValidations(EventCoordinates coordinates) {
        return delegate.getKeyStateWithEndorsementsAndValidations(coordinates);
    }

    @Override
    public CompletableFuture<Map<EventCoordinates, JohnHancock>> getValidations(EventCoordinates coordinates) {
        return delegate.getValidations(coordinates);
    }

    @Override
    public CompletableFuture<Verifier> getVerifier(KeyCoordinates coordinates) {
        return delegate.getVerifier(coordinates);
    }

    @Override
    public CompletableFuture<List<EventWithAttachments>> kerl(Identifier identifier) {
        return delegate.kerl(identifier);
    }
}
