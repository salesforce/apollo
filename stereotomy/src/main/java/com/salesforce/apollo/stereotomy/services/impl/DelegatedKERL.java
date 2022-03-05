/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.impl;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.KERLProvider;
import com.salesforce.apollo.stereotomy.services.KERLRecorder;

/**
 * @author hal.hildebrand
 *
 */
public class DelegatedKERL implements KERL {
    @SuppressWarnings("unused")
    private final KERLProvider provider;
    @SuppressWarnings("unused")
    private final KERLRecorder recorder;

    public DelegatedKERL(KERLRecorder recorder, KERLProvider provider) {
        this.recorder = recorder;
        this.provider = provider;
    }

    @Override
    public CompletableFuture<Void> append(AttachmentEvent event) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<KeyState> append(EventWithAttachments ewa) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Optional<Attachment> getAttachment(EventCoordinates coordinates) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(Digest digest) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
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

    @Override
    public Optional<List<EventWithAttachments>> kerl(Identifier identifier) {
        // TODO Auto-generated method stub
        return null;
    }
}
