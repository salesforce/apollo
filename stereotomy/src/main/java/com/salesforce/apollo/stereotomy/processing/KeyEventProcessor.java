/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.processing;

import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

/**
 * @author hal.hildebrand
 *
 */
public class KeyEventProcessor implements Validator, KeyEventVerifier {
    private final KERL                                     kerl;
    private final BiFunction<KeyState, KeyEvent, KeyState> keyStateProcessor;

    public KeyEventProcessor(KERL kerl) {
        this(kerl, new KeyStateProcessor(kerl));
    }

    public KeyEventProcessor(KERL kerl, BiFunction<KeyState, KeyEvent, KeyState> keyStateProcessor) {
        this.kerl = kerl;
        this.keyStateProcessor = keyStateProcessor;
    }

    public Attachment process(AttachmentEvent attachmentEvent) throws AttachmentEventProcessingException {
        KeyEvent event;
        try {
            event = kerl.getKeyEvent(attachmentEvent.coordinates()).get();
            if (event == null) {
                throw new MissingAttachmentEventException(attachmentEvent, attachmentEvent.coordinates());
            }
            var state = kerl.getKeyState(attachmentEvent.coordinates()).get();
            if (state == null) {
                throw new MissingAttachmentEventException(attachmentEvent, attachmentEvent.coordinates());
            }
            return verify(state, event, attachmentEvent.attachments());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new InvalidKeyEventException(String.format("Error processing: " + attachmentEvent), e.getCause());
        }
    }

    public KeyState process(KeyEvent event) throws KeyEventProcessingException {
        KeyState previousState = null;

        try {
            if (!(event instanceof InceptionEvent)) {
                previousState = kerl.getKeyState(event.getPrevious()).get();
                if (previousState == null) {
                    throw new MissingEventException(event, event.getPrevious());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new InvalidKeyEventException(String.format("Error processing: " + event), e.getCause());
        }

        return process(previousState, event);
    }

    public KeyState process(KeyState previousState, KeyEvent event) throws KeyEventProcessingException {

        validateKeyEventData(previousState, event, kerl);

        KeyState newState = keyStateProcessor.apply(previousState, event);

        return newState;
    }

    private Attachment verify(KeyState state, KeyEvent event, Attachment attachments) {
        return attachments; // TODO
    }
}
