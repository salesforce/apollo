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
        KeyEvent event = kerl.getKeyEvent(attachmentEvent.getCoordinates())
                             .orElseThrow(() -> new MissingEventException(attachmentEvent,
                                     attachmentEvent.getCoordinates()));
        var state = kerl.getKeyState(attachmentEvent.getCoordinates())
                        .orElseThrow(() -> new MissingReferencedEventException(attachmentEvent,
                                attachmentEvent.getCoordinates()));

        @SuppressWarnings("unused")
        var validControllerSignatures = verifyAuthentication(state, event, attachmentEvent.getAuthentication(), kerl);
        @SuppressWarnings("unused")
        var validWitnessReceipts = verifyEndorsements(state, event, attachmentEvent.getEndorsements());
        @SuppressWarnings("unused")
        var validOtherReceipts = verifyReceipts(event, attachmentEvent.getReceipts(), kerl);

        // TODO remove invalid signatures before appending
        kerl.append(attachmentEvent, state);
    }

    public KeyState process(KeyState previousState, KeyEvent event) throws KeyEventProcessingException {

        validateKeyEventData(previousState, event, kerl);

        KeyState newState = keyStateProcessor.apply(previousState, event);

        // TODO remove invalid signatures before appending
        kerl.append(event, newState);

        return newState;
    }

    public KeyState process(KeyEvent event) throws KeyEventProcessingException {
        KeyState previousState = null;

        if (!(event instanceof InceptionEvent)) {
            previousState = kerl.getKeyState(event.getPrevious())
                                .orElseThrow(() -> new MissingEventException(event, event.getPrevious()));
        }

        validateKeyEventData(previousState, event, kerl);

        KeyState newState = keyStateProcessor.apply(previousState, event);

        // TODO remove invalid signatures before appending
        kerl.append(event, newState);

        return newState;
    }
}
