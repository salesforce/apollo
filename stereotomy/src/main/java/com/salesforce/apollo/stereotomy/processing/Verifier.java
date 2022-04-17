/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.processing;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.Verifier.DefaultVerifier;
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

    default Map<Integer, JohnHancock> verifyEndorsements(KeyState state, KeyEvent event,
                                                         Map<Integer, JohnHancock> receipts) {
        var validReceipts = new HashMap<Integer, JohnHancock>();

        for (var entry : receipts.entrySet()) {
            var publicKey = state.getWitnesses().get(entry.getKey()).getPublicKey();

            var ops = SignatureAlgorithm.lookup(publicKey);
            if (ops.verify(publicKey, entry.getValue(), event.getBytes())) {
                validReceipts.put(entry.getKey(), entry.getValue());
            }
        }

        if (validReceipts.size() < state.getWitnessThreshold()) {
            throw new UnmetWitnessThresholdException(event);
        }

        return validReceipts;
    }
}
