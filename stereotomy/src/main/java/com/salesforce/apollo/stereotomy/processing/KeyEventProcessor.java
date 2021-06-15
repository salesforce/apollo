/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.processing;

import java.util.function.BiFunction;

import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.store.StateStore;

/**
 * @author hal.hildebrand
 *
 */
public class KeyEventProcessor {
    private final StateStore                               keyEventStore;
    private final Validator                                validator;
    private final Verifier                                 verifier;
    private final BiFunction<KeyState, KeyEvent, KeyState> keyStateProcessor;

    public KeyEventProcessor(StateStore keyEventStore, Verifier verifier, Validator validator,
            BiFunction<KeyState, KeyEvent, KeyState> keyStateProcessor) {
        this.keyEventStore = keyEventStore;
        this.validator = validator;
        this.verifier = verifier;
        this.keyStateProcessor = keyStateProcessor;
    }

    public KeyState process(KeyEvent event) throws KeyEventProcessingException {
        KeyState previousState = null;

        if (!(event instanceof InceptionEvent)) {
            previousState = keyEventStore.getKeyState(event.getPrevious())
                                         .orElseThrow(() -> new MissingEventException(event, event.getPrevious()));
        }

        validator.validateKeyEventData(previousState, event);

        KeyState newState = keyStateProcessor.apply(previousState, event);

        // TODO remove invalid signatures before appending
        keyEventStore.append(event, newState);

        return newState;
    }

    public void process(AttachmentEvent attachmentEvent) throws AttachmentEventProcessingException {
        KeyEvent event = keyEventStore.getKeyEvent(attachmentEvent.getCoordinates())
                                      .orElseThrow(() -> new MissingEventException(attachmentEvent,
                                              attachmentEvent.getCoordinates()));
        var state = keyEventStore.getKeyState(attachmentEvent.getCoordinates())
                                 .orElseThrow(() -> new MissingReferencedEventException(attachmentEvent,
                                         attachmentEvent.getCoordinates()));

        @SuppressWarnings("unused")
        var validControllerSignatures = verifier.verifyAuthentication(state, event, attachmentEvent.getAuthentication());
        @SuppressWarnings("unused")
        var validWitnessReceipts = verifier.verifyEndorsements(state, event, attachmentEvent.getEndorsements());
        @SuppressWarnings("unused")
        var validOtherReceipts = verifier.verifyReceipts(event, attachmentEvent.getReceipts());

        // TODO remove invalid signatures before appending
        keyEventStore.append(attachmentEvent, state);
    }
}
