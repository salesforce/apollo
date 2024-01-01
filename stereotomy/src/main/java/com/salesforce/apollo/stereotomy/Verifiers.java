/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.cryptography.SigningThreshold;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.cryptography.Verifier.DefaultVerifier;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

import java.io.InputStream;
import java.util.*;

/**
 * @author hal.hildebrand
 */
public interface Verifiers {

    Verifiers NONE = new Verifiers() {
        @Override
        public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
            return Optional.of(v());
        }

        @Override
        public Optional<Verifier> verifierFor(Identifier identifier) {
            return Optional.of(v());
        }

        Verifier v() {
            return new Verifier() {
                @Override
                public Filtered filtered(SigningThreshold threshold, JohnHancock signature, InputStream message) {
                    return new Filtered(false, 0, null);
                }

                @Override
                public boolean verify(JohnHancock signature, InputStream message) {
                    return true;
                }

                @Override
                public boolean verify(SigningThreshold threshold, JohnHancock signature, InputStream message) {
                    return true;
                }
            };
        }
    };

    static Verifiers from(KERL kerl) {
        return new Verifiers() {
            @Override
            public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
                return verifierFor(coordinates.getIdentifier());
            }

            @Override
            public Optional<Verifier> verifierFor(Identifier identifier) {
                return Optional.of(new KerlVerifier<>(identifier, kerl));
            }
        };
    }

    static Verifiers fromEvents(List<InceptionEvent> states) {
        return new FixedVerifiers(FixedVerifiers.fromEvents(states));
    }

    static Verifiers fromEventState(List<com.salesforce.apollo.stereotomy.event.proto.InceptionEvent> states) {
        return new FixedVerifiers(FixedVerifiers.fromEventState(states));
    }

    static Verifiers fromKeyState(List<KeyState> states) {
        return new FixedVerifiers(FixedVerifiers.fromKeyState(states));
    }

    static Verifiers fromKeyState_(List<KeyState_> states) {
        return new FixedVerifiers(FixedVerifiers.fromKeyState_(states));
    }

    Optional<Verifier> verifierFor(EventCoordinates coordinates);

    Optional<Verifier> verifierFor(Identifier identifier);

    class DelegatedVerifiers implements Verifiers {
        private volatile Verifiers delegate;

        public DelegatedVerifiers(Verifiers delegate) {
            this.delegate = delegate;
        }

        public void setDelegate(Verifiers delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<Verifier> verifierFor(Identifier identifier) {
            return delegate().verifierFor(identifier);
        }

        @Override
        public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
            return delegate().verifierFor(coordinates);
        }

        private Verifiers delegate() {
            final var current = delegate;
            return current;
        }
    }

    class FixedVerifiers implements Verifiers {
        private final Map<EventCoordinates, Verifier> verifiersByCoordinates;
        private final Map<Identifier, Verifier>       verifiersByIdentifer;

        public FixedVerifiers(Map<EventCoordinates, Verifier> verifiersByCoordinates,
                              Map<Identifier, Verifier> verifiersByIdentifer) {
            this.verifiersByCoordinates = verifiersByCoordinates;
            this.verifiersByIdentifer = verifiersByIdentifer;
        }

        private FixedVerifiers(Pair verifiers) {
            verifiersByCoordinates = verifiers.coords;
            verifiersByIdentifer = verifiers.ids;
        }

        private static Pair fromEvents(Collection<InceptionEvent> states) {
            Map<EventCoordinates, Verifier> coords = new HashMap<>();
            Map<Identifier, Verifier> ids = new HashMap<>();
            states.forEach(ks -> {
                coords.put(ks.getCoordinates(), new DefaultVerifier(ks.getKeys()));
            });
            states.forEach(ks -> {
                ids.put(ks.getIdentifier(), new DefaultVerifier(ks.getKeys()));
            });
            return new Pair(coords, ids);
        }

        private static Pair fromEventState(
        Collection<com.salesforce.apollo.stereotomy.event.proto.InceptionEvent> states) {
            return fromEvents(states.stream().map(ks -> ProtobufEventFactory.toKeyEvent(ks)).toList());
        }

        private static Pair fromKeyState(Collection<KeyState> states) {
            Map<EventCoordinates, Verifier> coords = new HashMap<>();
            Map<Identifier, Verifier> ids = new HashMap<>();
            states.forEach(ks -> {
                coords.put(ks.getCoordinates(), new DefaultVerifier(ks.getKeys()));
            });
            states.forEach(ks -> {
                ids.put(ks.getIdentifier(), new DefaultVerifier(ks.getKeys()));
            });
            return new Pair(coords, ids);
        }

        private static Pair fromKeyState_(Collection<KeyState_> states) {
            return fromKeyState(states.stream().map(ks -> new KeyStateImpl(ks)).map(ks -> (KeyState) ks).toList());
        }

        @Override
        public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
            return Optional.ofNullable(verifiersByCoordinates.get(coordinates));
        }

        @Override
        public Optional<Verifier> verifierFor(Identifier identifier) {
            return Optional.ofNullable(verifiersByIdentifer.get(identifier));
        }

        record Pair(Map<EventCoordinates, Verifier> coords, Map<Identifier, Verifier> ids) {
        }
    }

}
