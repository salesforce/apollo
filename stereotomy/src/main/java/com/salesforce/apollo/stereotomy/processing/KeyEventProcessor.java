/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.processing;

import java.util.function.BiFunction;

import com.salesforce.apollo.stereotomy.KeyEventLog;
import com.salesforce.apollo.stereotomy.KeyEventReceiptLog;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

/**
 * @author hal.hildebrand
 *
 */
public class KeyEventProcessor {
    private final KeyEventLog                              kel;
    private final KeyEventReceiptLog                       kerl;
    private final BiFunction<KeyState, KeyEvent, KeyState> keyStateProcessor;
    private final Validator                                validator;
    private final Verifier                                 verifier;

    public KeyEventProcessor(KeyEventLog kel, KeyEventReceiptLog kerl) {
        this(kel, kerl, new Verifier(kel), new Validator(kel), new KeyStateProcessor(kel));
    }

    public KeyEventProcessor(KeyEventLog kel, KeyEventReceiptLog kerl, Verifier verifier, Validator validator,
            BiFunction<KeyState, KeyEvent, KeyState> keyStateProcessor) {
        this.kel = kel;
        this.kerl = kerl;
        this.validator = validator;
        this.verifier = verifier;
        this.keyStateProcessor = keyStateProcessor;
    }

    public void process(AttachmentEvent attachmentEvent) throws AttachmentEventProcessingException {
        KeyEvent event = kel.getKeyEvent(attachmentEvent.getCoordinates())
                            .orElseThrow(() -> new MissingEventException(attachmentEvent,
                                    attachmentEvent.getCoordinates()));
        var state = kel.getKeyState(attachmentEvent.getCoordinates())
                       .orElseThrow(() -> new MissingReferencedEventException(attachmentEvent,
                               attachmentEvent.getCoordinates()));

        @SuppressWarnings("unused")
        var validControllerSignatures = verifier.verifyAuthentication(state, event,
                                                                      attachmentEvent.getAuthentication());
        @SuppressWarnings("unused")
        var validWitnessReceipts = verifier.verifyEndorsements(state, event, attachmentEvent.getEndorsements());
        @SuppressWarnings("unused")
        var validOtherReceipts = verifier.verifyReceipts(event, attachmentEvent.getReceipts());

        // TODO remove invalid signatures before appending
        kerl.append(attachmentEvent, state);
    }

    public KeyState process(KeyState previousState, KeyEvent event) throws KeyEventProcessingException { 

        validator.validateKeyEventData(previousState, event);

        KeyState newState = keyStateProcessor.apply(previousState, event);

        // TODO remove invalid signatures before appending
        kel.append(event, newState);

        return newState;
    }

    public KeyState process(KeyEvent event) throws KeyEventProcessingException {
        KeyState previousState = null;

        if (!(event instanceof InceptionEvent)) {
            previousState = kel.getKeyState(event.getPrevious())
                               .orElseThrow(() -> new MissingEventException(event, event.getPrevious()));
        }

        validator.validateKeyEventData(previousState, event);

        KeyState newState = keyStateProcessor.apply(previousState, event);

        // TODO remove invalid signatures before appending
        kel.append(event, newState);

        return newState;
    }
}
