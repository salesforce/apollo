/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.processing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Verifier.DefaultVerifier;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KEL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

/**
 * @author hal.hildebrand
 *
 */
public interface Verifier {
    static final Logger log = LoggerFactory.getLogger(Verifier.class);

    default JohnHancock verifyAuthentication(KeyState state, KeyEvent event, JohnHancock signatures, KEL kel) {
        Optional<KeyEvent> lookup = kel.getKeyEvent(state.getLastEstablishmentEvent());
        if (lookup.isEmpty()) {
            throw new MissingEstablishmentEventException(event, state.getLastEstablishmentEvent());
        }
        var kee = (EstablishmentEvent) lookup.get();
        var filtered = new DefaultVerifier(kee.getKeys()).filtered(kee.getSigningThreshold(), signatures,
                                                                   event.getBytes());
        if (!filtered.verified()) {
            throw new UnmetSigningThresholdException(event);
        }

        return filtered.filtered();
    }

    default List<JohnHancock> verifyEndorsements(KeyState state, KeyEvent event, List<JohnHancock> receipts) {
        var validReceipts = new ArrayList<JohnHancock>();

        int witnessIndex = 0;
        for (var signature : receipts) {
            var publicKey = state.getWitnesses().get(witnessIndex).getPublicKey();

            var ops = SignatureAlgorithm.lookup(publicKey);
            if (ops.verify(publicKey, signature, event.getBytes())) {
                validReceipts.add(signature);
            } else {
                log.debug("invalid receipt from witness {}", witnessIndex);
                validReceipts.add(null);
            }
            witnessIndex++;
        }

        if (validReceipts.size() < state.getWitnessThreshold()) {
            throw new UnmetWitnessThresholdException(event);
        }

        return validReceipts;
    }

    default Map<EventCoordinates, JohnHancock> verifyReceipts(KeyEvent event,
                                                              Map<EventCoordinates, JohnHancock> otherReceipts,
                                                              KEL kel) {
        var verified = new HashMap<EventCoordinates, JohnHancock>();
        for (var kv : otherReceipts.entrySet()) {
            // TODO escrow or something
            Optional<KeyState> keyState = kel.getKeyState(kv.getKey());

            if (keyState.isEmpty()) {
                continue;
            }
            verified.put(kv.getKey(), verifyAuthentication(keyState.get(), event, kv.getValue(), kel));
        }

        return verified;
    }
}
