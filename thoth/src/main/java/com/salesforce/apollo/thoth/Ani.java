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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
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
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.KeyStateWithEndorsementsAndValidations;
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

    public static Caffeine<EventCoordinates, Boolean> defaultValidatedBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((EventCoordinates coords, Boolean validated,
                                         RemovalCause cause) -> log.trace("Validation {} was removed ({})", coords,
                                                                          cause));
    }

    private final KERL                                         kerl;
    private final SigningMember                                member;
    private final SigningThreshold                             threshold;
    private final AsyncLoadingCache<EventCoordinates, Boolean> validated;
    private final Set<Identifier>                              validators;

    public Ani(SigningMember member, SigningThreshold threshold, Duration validationTimeout, KERL kerl,
               Caffeine<EventCoordinates, Boolean> validatedBuilder, List<Identifier> validators) {
        validated = validatedBuilder.buildAsync(new AsyncCacheLoader<>() {
            @Override
            public CompletableFuture<? extends Boolean> asyncLoad(EventCoordinates key,
                                                                  Executor executor) throws Exception {
                return performValidation(key, validationTimeout);
            }
        });
        this.member = member;
        this.kerl = kerl;
        this.threshold = threshold;
        this.validators = new HashSet<>(validators);
    }

    public Ani(SigningMember member, SigningThreshold threshold, Duration validationTimeout, KERL kerl,
               List<Identifier> validators) {
        this(member, threshold, validationTimeout, kerl, defaultValidatedBuilder(), validators);
    }

    public void clearValidations() {
        validated.synchronous().invalidateAll();
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
                    return new Filtered(false, null);
                } catch (ExecutionException e) {
                    log.error("Unable to validate: {} on: {}", coordinates, member, e.getCause());
                    return new Filtered(false, null);
                } catch (TimeoutException e) {
                    log.error("Timeout validating: {} on: {} ", coordinates, member);
                    return new Filtered(false, null);
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
                    return Ani.this.validate(event).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
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
                               .thenCompose(ke -> Ani.this.validate(ke))
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

    public CompletableFuture<Boolean> validate(KeyEvent event) {
        return validated.get(event.getCoordinates());
    }

    KERL getKerl() {
        return kerl;
    }

    private CompletableFuture<Boolean> complete(boolean result) {
        var fs = new CompletableFuture<Boolean>();
        fs.complete(result);
        return fs;
    }

    private CompletableFuture<Boolean> performValidation(EventCoordinates coord, Duration timeout) {
        return kerl.getKeyEvent(coord)
                   .thenCombine(kerl.getKeyStateWithEndorsementsAndValidations(coord), (event, ksa) -> {
                       try {
                           return validate(timeout, ksa, event).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                       } catch (InterruptedException | TimeoutException e) {
                           throw new IllegalStateException(e);
                       } catch (ExecutionException e) {
                           throw new IllegalStateException(e.getCause());
                       }
                   });
    }

    private CompletableFuture<Boolean> validate(Duration timeout, KeyStateWithEndorsementsAndValidations ksAttach,
                                                KeyEvent event) {
        // TODO Multisig
        var state = ksAttach.state();
        boolean witnessed = false;
        if (state.getWitnesses().isEmpty()) {
            witnessed = true; // no witnesses for event
        } else {
            var witnesses = new PublicKey[state.getWitnesses().size()];
            for (var i = 0; i < state.getWitnesses().size(); i++) {
                witnesses[i] = state.getWitnesses().get(i).getPublicKey();
            }
            var algo = SignatureAlgorithm.lookup(witnesses[0]);
            byte[][] signatures = new byte[state.getWitnesses().size()][];
            if (!ksAttach.endorsements().isEmpty()) {
                for (var entry : ksAttach.endorsements().entrySet()) {
                    signatures[entry.getKey()] = entry.getValue().getBytes()[0];
                }
            }
            witnessed = new JohnHancock(algo, signatures).verify(state.getSigningThreshold(), witnesses,
                                                                 BbBackedInputStream.aggregate(event.toKeyEvent_()
                                                                                                    .toByteString()));
        }

        if (!witnessed) {
            return complete(witnessed);
        }

        record resolved(KeyState state, JohnHancock signature) {}
        var mapped = new CopyOnWriteArrayList<resolved>();
        var last = ksAttach.validations()
                           .entrySet()
                           .stream()
                           .filter(e -> validators.contains(e.getKey()))
                           .map(e -> kerl.getKeyState(e.getKey()).thenApply(ks -> {
                               mapped.add(new resolved(ks, e.getValue()));
                               return ks;
                           }))
                           .reduce((a, b) -> a.thenCompose(ks -> b));

        if (last.isEmpty()) {
            log.trace("No mapped validations for {} on: {}", ksAttach.state().getCoordinates(), member.getId());
            return complete(SigningThreshold.thresholdMet(threshold, new int[] {}));
        }

        return last.get().thenApply(o -> {
            log.trace("Evaluating validation {} validations: {} mapped: {} on: {}", ksAttach.state().getCoordinates(),
                      ksAttach.validations().size(), mapped.size(), member.getId());
            var validations = new PublicKey[mapped.size()];
            byte[][] signatures = new byte[mapped.size()][];

            int index = 0;
            for (var r : mapped) {
                validations[index] = r.state.getKeys().get(0);
                signatures[index++] = r.signature.getBytes()[0];
            }

            SignatureAlgorithm algo = SignatureAlgorithm.lookup(validations[0]);
            var validated = new JohnHancock(algo,
                                            signatures).verify(threshold, validations,
                                                               BbBackedInputStream.aggregate(event.toKeyEvent_()
                                                                                                  .toByteString()));
            return validated;
        }).exceptionally(t -> {
            log.error("Error in validating {} on: {}", ksAttach.state().getCoordinates(), member.getId(), t);
            return false;
        });
    }
}
