/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.processing;

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
public class KeyEventProcessor implements Validator, Verifier {
    private final KERL                                     kerl;
    private final BiFunction<KeyState, KeyEvent, KeyState> keyStateProcessor;

    public KeyEventProcessor(KERL kerl) {
        this(kerl, new KeyStateProcessor(kerl));
    }

    public KeyEventProcessor(KERL kerl, BiFunction<KeyState, KeyEvent, KeyState> keyStateProcessor) {
        this.kerl = kerl;
        this.keyStateProcessor = keyStateProcessor;
    }

    public void process(AttachmentEvent attachmentEvent) throws AttachmentEventProcessingException {
        KeyEvent event = kerl.getKeyEvent(attachmentEvent.coordinates())
                             .orElseThrow(() -> new MissingAttachmentEventException(attachmentEvent,
                                                                                    attachmentEvent.coordinates()));
        var state = kerl.getKeyState(attachmentEvent.coordinates())
                        .orElseThrow(() -> new MissingReferencedEventException(attachmentEvent,
                                                                               attachmentEvent.coordinates()));

        @SuppressWarnings("unused")
        var attachments = verify(state, event, attachmentEvent.attachments());

        // TODO remove invalid signatures before appending
        kerl.append(attachmentEvent, state);
    }

    private Attachment verify(KeyState state, KeyEvent event, Attachment attachments) {
        // TODO Auto-generated method stub
        return null;
    }

    public KeyState process(KeyState previousState, KeyEvent event) throws KeyEventProcessingException {

        validateKeyEventData(previousState, event, kerl);

        KeyState newState = keyStateProcessor.apply(previousState, event);

        return newState;
    }

    public KeyState process(KeyEvent event) throws KeyEventProcessingException {
        KeyState previousState = null;

        if (!(event instanceof InceptionEvent)) {
            previousState = kerl.getKeyState(event.getPrevious())
                                .orElseThrow(() -> new MissingEventException(event, event.getPrevious()));
        }

        return process(previousState, event);
    }
}
