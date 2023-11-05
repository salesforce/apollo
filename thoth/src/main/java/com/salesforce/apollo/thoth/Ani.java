/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import com.salesforce.apollo.crypto.*;
import com.salesforce.apollo.crypto.Verifier.Filtered;
import com.salesforce.apollo.crypto.ssl.CertificateValidator;
import com.salesforce.apollo.stereotomy.*;
import com.salesforce.apollo.stereotomy.KEL.KeyStateWithAttachments;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.utils.BbBackedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
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

    private final Digest id;
    private final KERL kerl;

    public Ani(Digest id, KERL kerl) {
        this.id = id;
        this.kerl = kerl;
    }

    public CertificateValidator certificateValidator(Duration timeout) {
        return new StereotomyValidator(verifiers(timeout));
    }

    public EventValidation eventValidation(Duration timeout) {
        return new EventValidation() {
            @Override
            public Filtered filtered(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                                     InputStream message) {
                KeyState ks = kerl.getKeyState(coordinates);
                var v = new Verifier.DefaultVerifier(ks.getKeys());
                return v.filtered(threshold, signature, message);
            }

            @Override
            public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
                return Optional.of(kerl.getKeyState(coordinates));
            }

            @Override
            public boolean validate(EstablishmentEvent event) {
                return Ani.this.validateKerl(event, timeout);
            }

            @Override
            public boolean validate(EventCoordinates coordinates) {
                KeyEvent ke = kerl.getKeyEvent(coordinates);
                return Ani.this.validateKerl(ke, timeout);
            }

            @Override
            public boolean verify(EventCoordinates coordinates, JohnHancock signature, InputStream message) {
                KeyState ks = kerl.getKeyState(coordinates);
                var v = new Verifier.DefaultVerifier(ks.getKeys());
                return v.verify(signature, message);
            }

            @Override
            public boolean verify(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                                  InputStream message) {
                KeyState ks = kerl.getKeyState(coordinates);
                var v = new Verifier.DefaultVerifier(ks.getKeys());
                return v.verify(threshold, signature, message);
            }
        };
    }

    public Verifiers verifiers(Duration timeout) {
        return new Verifiers() {

            @Override
            public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
                EstablishmentEvent ke = (EstablishmentEvent) kerl.getKeyEvent(coordinates);
                return Optional.ofNullable(new Verifier.DefaultVerifier(ke.getKeys()));
            }

            @Override
            public Optional<Verifier> verifierFor(Identifier identifier) {
                EstablishmentEvent ke = (EstablishmentEvent) kerl.getKeyState(identifier);
                return Optional.ofNullable(new Verifier.DefaultVerifier(ke.getKeys()));
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
            witnessed = new JohnHancock(algo, signatures).verify(state.getSigningThreshold(), witnesses,
                    BbBackedInputStream.aggregate(event.toKeyEvent_()
                            .toByteString()));
        }
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
