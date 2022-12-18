/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import java.io.InputStream;
import java.security.PublicKey;
import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.crypto.Verifier.Filtered;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.KEL.KeyStateWithAttachments;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.Verifiers;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * Key Event Validation
 *
 * @author hal.hildebrand
 *
 */
public class Ani {

    private static final Logger log = LoggerFactory.getLogger(Ani.class);

    public static Caffeine<EventCoordinates, Boolean> defaultKerlValidatedBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((EventCoordinates coords, Boolean validated,
                                         RemovalCause cause) -> log.info("KERL validation {} was removed ({})", coords,
                                                                         cause));
    }

    public static Caffeine<EventCoordinates, Boolean> defaultRootValidatedBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((EventCoordinates coords, Boolean validated,
                                         RemovalCause cause) -> log.info("Root validation {} was removed ({})", coords,
                                                                         cause));
    }

    private final KERL          kerl;
    private final SigningMember member;

    public Ani(SigningMember member, Duration validationTimeout, KERL kerl) {
        this.member = member;
        this.kerl = kerl;
    }

    public EventValidation eventValidation(Duration timeout) {
        return new EventValidation() {
            @Override
            public Filtered filtered(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                                     InputStream message) {
                try {
                    return kerl.getKeyState(coordinates)
                               .thenApply(ks -> new Verifier.DefaultVerifier(ks.getKeys()))
                               .thenApply(v -> v.filtered(threshold, signature, message))
                               .get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return new Filtered(false, 0, null);
                } catch (ExecutionException e) {
                    log.error("Unable to validate: {} on: {}", coordinates, member, e.getCause());
                    return new Filtered(false, 0, null);
                } catch (TimeoutException e) {
                    log.error("Timeout validating: {} on: {} ", coordinates, member);
                    return new Filtered(false, 0, null);
                }
            }

            @Override
            public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
                try {
                    return Optional.of(kerl.getKeyState(coordinates).get(timeout.toNanos(), TimeUnit.NANOSECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                } catch (ExecutionException e) {
                    log.error("Unable to retrieve keystate: {} on: {}", coordinates, member, e.getCause());
                    return Optional.empty();
                } catch (TimeoutException e) {
                    log.error("Timeout retrieving keystate: {} on: {} ", coordinates, member);
                    return Optional.empty();
                }
            }

            @Override
            public boolean validate(EstablishmentEvent event) {
                try {
                    return Ani.this.validateKerl(event, timeout).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } catch (ExecutionException e) {
                    log.error("Unable to validate: {} on: {}", event.getCoordinates(), member, e.getCause());
                    return false;
                } catch (TimeoutException e) {
                    log.error("Timeout validating: {} on: {} ", event.getCoordinates(), member);
                    return false;
                }
            }

            @Override
            public boolean validate(EventCoordinates coordinates) {
                try {
                    return kerl.getKeyEvent(coordinates)
                               .thenCompose(ke -> Ani.this.validateKerl(ke, timeout))
                               .get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } catch (ExecutionException e) {
                    log.error("Unable to validate: {} on: {}", coordinates, member, e.getCause());
                    return false;
                } catch (TimeoutException e) {
                    log.error("Timeout validating: {} on: {} ", coordinates, member);
                    return false;
                }
            }

            @Override
            public boolean verify(EventCoordinates coordinates, JohnHancock signature, InputStream message) {
                try {
                    return kerl.getKeyState(coordinates)
                               .thenApply(ks -> new Verifier.DefaultVerifier(ks.getKeys()))
                               .thenApply(v -> v.verify(signature, message))
                               .get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } catch (ExecutionException e) {
                    log.error("Unable to validate: {} on: {}", coordinates, member, e.getCause());
                    return false;
                } catch (TimeoutException e) {
                    log.error("Timeout validating: {} on: {} ", coordinates, member);
                    return false;
                }
            }

            @Override
            public boolean verify(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                                  InputStream message) {
                try {
                    return kerl.getKeyState(coordinates)
                               .thenApply(ks -> new Verifier.DefaultVerifier(ks.getKeys()))
                               .thenApply(v -> v.verify(threshold, signature, message))
                               .get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } catch (ExecutionException e) {
                    log.error("Unable to validate: {} on: {}", coordinates, member, e.getCause());
                    return false;
                } catch (TimeoutException e) {
                    log.error("Timeout validating: {} on: {} ", coordinates, member);
                    return false;
                }
            }
        };
    }

    public Verifiers verifiers(Duration timeout) {
        return new Verifiers() {

            @Override
            public Optional<Verifier> verifierFor(EventCoordinates coordinates) {
                try {
                    return Optional.ofNullable(kerl.getKeyEvent(coordinates)
                                                   .thenApply(ke -> (EstablishmentEvent) ke)
                                                   .thenApply(ke -> new Verifier.DefaultVerifier(ke.getKeys()))
                                                   .get(timeout.toNanos(), TimeUnit.NANOSECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                } catch (ExecutionException e) {
                    log.error("Unable to validate: {} on: {}", coordinates, member, e.getCause());
                    return Optional.empty();
                } catch (TimeoutException e) {
                    log.error("Timeout validating: {} on: {} ", coordinates, member);
                    return Optional.empty();
                }
            }

            @Override
            public Optional<Verifier> verifierFor(Identifier identifier) {
                try {
                    return Optional.ofNullable(kerl.getKeyState(identifier)
                                                   .thenApply(ke -> (EstablishmentEvent) ke)
                                                   .thenApply(ke -> new Verifier.DefaultVerifier(ke.getKeys()))
                                                   .get(timeout.toNanos(), TimeUnit.NANOSECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                } catch (ExecutionException e) {
                    log.error("Unable to validate: {} on: {}", identifier, member, e.getCause());
                    return Optional.empty();
                } catch (TimeoutException e) {
                    log.error("Timeout validating: {} on: {} ", identifier, member);
                    return Optional.empty();
                }
            }
        };
    }

    private CompletableFuture<Boolean> complete(boolean result) {
        var fs = new CompletableFuture<Boolean>();
        fs.complete(result);
        return fs;
    }

    private CompletableFuture<Boolean> kerlValidate(Duration timeout, KeyStateWithAttachments ksa, KeyEvent event) {
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
        return complete(witnessed);
    }

    private CompletableFuture<Boolean> performKerlValidation(EventCoordinates coord, Duration timeout) {
        return kerl.getKeyEvent(coord).thenCombine(kerl.getKeyStateWithAttachments(coord), (event, ksa) -> {
            try {
                return kerlValidate(timeout, ksa, event).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
            } catch (InterruptedException | TimeoutException e) {
                throw new IllegalStateException(e);
            } catch (ExecutionException e) {
                throw new IllegalStateException(e.getCause());
            }
        });
    }

    private CompletableFuture<Boolean> validateKerl(KeyEvent event, Duration timeout) {
        return performKerlValidation(event.getCoordinates(), timeout);
    }
}
