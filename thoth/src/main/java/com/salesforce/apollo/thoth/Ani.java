/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SignatureAlgorithm;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.cryptography.ssl.CertificateValidator;
import com.salesforce.apollo.stereotomy.*;
import com.salesforce.apollo.stereotomy.KEL.KeyStateWithAttachments;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.utils.BbBackedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PublicKey;
import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;

/**
 * Stereotomy key event validation, certificate validator and verifiers
 *
 * @author hal.hildebrand
 */
public class Ani {

    private static final Logger log = LoggerFactory.getLogger(Ani.class);

    private final Digest member;
    private final KERL   kerl;

    public Ani(Digest member, KERL kerl) {
        this.member = member;
        this.kerl = kerl;
    }

    public CertificateValidator certificateValidator(Duration timeout) {
        return new StereotomyValidator(verifiers(timeout));
    }

    public EventValidation eventValidation(Duration timeout) {
        return new EventValidation() {

            @Override
            public boolean validate(EstablishmentEvent event) {
                log.trace("Validate event: {} on: {}", event.getCoordinates(), member);
                var result = Ani.this.validateKerl(event, timeout);
                log.info("Validate event: {}: {} on: {}", event, result, member);
                return result;
            }

            @Override
            public boolean validate(Identifier identifier) {
                log.trace("Validating identifier: {} on: {}", identifier, member);
                var ks = kerl.getKeyState(identifier);
                var ke = kerl.getKeyEvent(ks.getLastEstablishmentEvent());
                var result = Ani.this.validateKerl(ke, timeout);
                log.info("Validating identifier: {}:{} on: {}", identifier, result, member);
                return result;
            }
        };
    }

    public Verifiers verifiers(Duration timeout) {
        return new Verifiers() {

            @Override
            public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
                return Optional.of(new KerlVerifier<>(coordinates.getIdentifier(), kerl));
            }

            @Override
            public Optional<Verifier> verifierFor(Identifier identifier) {
                return Optional.of(new KerlVerifier<>(identifier, kerl));
            }
        };
    }

    private boolean kerlValidate(Duration timeout, KeyStateWithAttachments ksa, KeyEvent event) {
        // TODO Multisig
        var state = ksa.state();
        boolean witnessed = false;
        if (state.getWitnesses().isEmpty()) {
            witnessed = true; // no witnesses for event
        } else {
            SignatureAlgorithm algo = null;
            var witnesses = new HashMap<Integer, PublicKey>();
            for (var i = 0; i < state.getWitnesses().size(); i++) {
                final PublicKey publicKey = state.getWitnesses().get(i).getPublicKey();
                witnesses.put(i, publicKey);
                if (algo == null) {
                    algo = SignatureAlgorithm.lookup(publicKey);
                }
            }
            byte[][] signatures = new byte[state.getWitnesses().size()][];
            final var endorsements = ksa.attachments().endorsements();
            if (!endorsements.isEmpty()) {
                for (var entry : endorsements.entrySet()) {
                    signatures[entry.getKey()] = entry.getValue().getBytes()[0];
                }
            }
            witnessed = new JohnHancock(algo, signatures, state.getSequenceNumber()).verify(state.getSigningThreshold(),
                                                                                            witnesses,
                                                                                            BbBackedInputStream.aggregate(
                                                                                            event.toKeyEvent_()
                                                                                                 .toByteString()));
        }
        log.trace("Kerl validation: {} for: {} on: {}", witnessed, ksa.state().getCoordinates(), member);
        return witnessed;
    }

    private boolean performKerlValidation(EventCoordinates coord, Duration timeout) {
        var event = kerl.getKeyEvent(coord);
        var ksa = kerl.getKeyStateWithAttachments(coord);
        return kerlValidate(timeout, ksa, event);
    }

    private boolean validateKerl(KeyEvent event, Duration timeout) {
        return performKerlValidation(event.getCoordinates(), timeout);
    }
}
