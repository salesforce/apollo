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
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KEL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.SigningThreshold;

/**
 * @author hal.hildebrand
 *
 */
public interface Verifier {
    static final Logger log = LoggerFactory.getLogger(Verifier.class);

    default HashMap<Integer, JohnHancock> verifyAuthentication(KeyState state, KeyEvent event,
                                                               Map<Integer, JohnHancock> signatures, KEL kel) {
        Optional<KeyEvent> lookup = kel.getKeyEvent(state.getLastEstablishmentEvent());
        if (lookup.isEmpty()) {
            throw new MissingEstablishmentEventException(event, state.getLastEstablishmentEvent());
        }
        var kee = (EstablishmentEvent) lookup.get();

        var verifiedSignatures = new HashMap<Integer, JohnHancock>();
        for (var kv : signatures.entrySet()) {
            var keyIndex = kv.getKey();

            if (keyIndex < 0 || keyIndex >= kee.getKeys().size()) {
                log.debug("signature keyIndex out of range: {}", keyIndex);
                continue;
            }

            var publicKey = kee.getKeys().get(kv.getKey());
            var signature = kv.getValue();

            var ops = SignatureAlgorithm.lookup(publicKey);
            if (ops.verify(publicKey, signature, event.getBytes())) {
                verifiedSignatures.put(keyIndex, signature);
            } else {
                log.debug("signature invalid: {}", keyIndex);
            }
        }

        var arrIndexes = verifiedSignatures.keySet().stream().mapToInt(Integer::intValue).toArray();
        if (!SigningThreshold.thresholdMet(kee.getSigningThreshold(), arrIndexes)) {
            throw new UnmetSigningThresholdException(event);
        }

        return verifiedSignatures;
    }

    default Map<Integer, JohnHancock> verifyEndorsements(KeyState state, KeyEvent event,
                                                         Map<Integer, JohnHancock> receipts) {
        var validReceipts = new HashMap<Integer, JohnHancock>();
        for (var kv : receipts.entrySet()) {
            var witnessIndex = kv.getKey();

            if (witnessIndex < 0 || witnessIndex >= state.getWitnesses().size()) {
                log.debug("endorsement index out of range: {}", witnessIndex);
                continue;
            }

            var publicKey = state.getWitnesses().get(witnessIndex).getPublicKey();
            var signature = kv.getValue();

            var ops = SignatureAlgorithm.lookup(publicKey);
            if (ops.verify(publicKey, signature, event.getBytes())) {
                validReceipts.put(witnessIndex, signature);
            } else {
                log.debug("invalid receipt from witness {}", witnessIndex);
            }
        }

        if (validReceipts.size() < state.getWitnessThreshold()) {
            throw new UnmetWitnessThresholdException(event);
        }

        return validReceipts;
    }

    default Map<EventCoordinates, Map<Integer, JohnHancock>> verifyReceipts(KeyEvent event,
                                                                            Map<EventCoordinates, Map<Integer, JohnHancock>> otherReceipts,
                                                                            KEL kel) {
        var verified = new HashMap<EventCoordinates, Map<Integer, JohnHancock>>();
        for (var kv : otherReceipts.entrySet()) {
            // TODO escrow or something
            Optional<KeyState> keyState = kel.getKeyState(kv.getKey());

            if (keyState.isEmpty()) {
                continue;
            }

            var verifiedSignatures = verifyAuthentication(keyState.get(), event, kv.getValue(), kel);
            verified.put(kv.getKey(), verifiedSignatures);
        }

        return verified;
    }
}
