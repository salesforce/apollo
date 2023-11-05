/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.processing;

import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

import java.util.function.BiFunction;

/**
 * @author hal.hildebrand
 */
public class KeyEventProcessor implements Validator, KeyEventVerifier {
    private final KERL kerl;
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
        event = kerl.getKeyEvent(attachmentEvent.coordinates());
        if (event == null) {
            throw new MissingAttachmentEventException(attachmentEvent, attachmentEvent.coordinates());
        }
        var state = kerl.getKeyState(attachmentEvent.coordinates());
        if (state == null) {
            throw new MissingAttachmentEventException(attachmentEvent, attachmentEvent.coordinates());
        }
        return verify(state, event, attachmentEvent.attachments());
    }

    public KeyState process(KeyEvent event) throws KeyEventProcessingException {
        KeyState previousState = null;

        if (!(event instanceof InceptionEvent)) {
            previousState = kerl.getKeyState(event.getPrevious());
            if (previousState == null) {
                throw new MissingEventException(event, event.getPrevious());
            }
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
