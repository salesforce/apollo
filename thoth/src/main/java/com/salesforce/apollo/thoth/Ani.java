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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

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
import com.salesforce.apollo.stereotomy.Verifiers;
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

    private final KERL                                         kerl;
    private final Supplier<SigningThreshold>                   kerlThreshold;
    private final AsyncLoadingCache<EventCoordinates, Boolean> kerlValidated;
    private final SigningMember                                member;
    private final Supplier<Set<Identifier>>                    roots;
    private final Supplier<SigningThreshold>                   rootThreshold;
    private final AsyncLoadingCache<EventCoordinates, Boolean> rootValidated;

    public Ani(SigningMember member, Duration validationTimeout, KERL kerl,
               Caffeine<EventCoordinates, Boolean> rootValidatedBuilder, Supplier<SigningThreshold> rootThreshold,
               Supplier<Set<Identifier>> roots, Caffeine<EventCoordinates, Boolean> kerlValidatedBuilder,
               Supplier<SigningThreshold> kerlThreshold) {
        rootValidated = rootValidatedBuilder.buildAsync(new AsyncCacheLoader<>() {
            @Override
            public CompletableFuture<? extends Boolean> asyncLoad(EventCoordinates key,
                                                                  Executor executor) throws Exception {
                return performRootValidation(key, validationTimeout);
            }
        });
        kerlValidated = kerlValidatedBuilder.buildAsync(new AsyncCacheLoader<>() {
            @Override
            public CompletableFuture<? extends Boolean> asyncLoad(EventCoordinates key,
                                                                  Executor executor) throws Exception {
                return performKerlValidation(key, validationTimeout);
            }
        });
        this.member = member;
        this.kerl = kerl;
        this.rootThreshold = rootThreshold;
        this.kerlThreshold = kerlThreshold;
        this.roots = roots;
    }

    public Ani(SigningMember member, Duration validationTimeout, KERL kerl, Supplier<SigningThreshold> rootThreshold,
               Supplier<Set<Identifier>> roots, Supplier<SigningThreshold> kerlThreshold) {
        this(member, validationTimeout, kerl, defaultRootValidatedBuilder(), rootThreshold, roots,
             defaultKerlValidatedBuilder(), kerlThreshold);
    }

    public void clearValidations() {
        rootValidated.synchronous().invalidateAll();
        kerlValidated.synchronous().invalidateAll();
    }

    public EventValidation eventRootValidation(Duration timeout) {
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
                    return Ani.this.validateRoot(event).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
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
                               .thenCompose(ke -> Ani.this.validateRoot(ke))
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
                    return Ani.this.validateKerl(event).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
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
                               .thenCompose(ke -> Ani.this.validateKerl(ke))
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

    public CompletableFuture<Boolean> validateKerl(KeyEvent event) {
        return kerlValidated.get(event.getCoordinates());
    }

    public CompletableFuture<Boolean> validateRoot(KeyEvent event) {
        return rootValidated.get(event.getCoordinates());
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

    KERL getKerl() {
        return kerl;
    }

    private CompletableFuture<Boolean> complete(boolean result) {
        var fs = new CompletableFuture<Boolean>();
        fs.complete(result);
        return fs;
    }

    private CompletableFuture<Boolean> kerlValidate(Duration timeout, KeyStateWithEndorsementsAndValidations ksAttach,
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
        var last = ksAttach.validations().entrySet().stream().map(e -> kerl.getKeyState(e.getKey()).thenApply(ks -> {
            mapped.add(new resolved(ks, e.getValue()));
            return ks;
        })).reduce((a, b) -> a.thenCompose(ks -> b));

        if (last.isEmpty()) {
            log.trace("No mapped validations for {} on: {}", ksAttach.state().getCoordinates(), member.getId());
            return complete(SigningThreshold.thresholdMet(kerlThreshold.get(), new int[] {}));
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
                                            signatures).verify(kerlThreshold.get(), validations,
                                                               BbBackedInputStream.aggregate(event.toKeyEvent_()
                                                                                                  .toByteString()));
            return validated;
        }).exceptionally(t -> {
            log.error("Error in validating {} on: {}", ksAttach.state().getCoordinates(), member.getId(), t);
            return false;
        });
    }

    private CompletableFuture<Boolean> performKerlValidation(EventCoordinates coord, Duration timeout) {
        return kerl.getKeyEvent(coord)
                   .thenCombine(kerl.getKeyStateWithEndorsementsAndValidations(coord), (event, ksa) -> {
                       try {
                           return kerlValidate(timeout, ksa, event).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                       } catch (InterruptedException | TimeoutException e) {
                           throw new IllegalStateException(e);
                       } catch (ExecutionException e) {
                           throw new IllegalStateException(e.getCause());
                       }
                   });
    }

    private CompletableFuture<Boolean> performRootValidation(EventCoordinates coord, Duration timeout) {
        return kerl.getKeyEvent(coord)
                   .thenCombine(kerl.getKeyStateWithEndorsementsAndValidations(coord), (event, ksa) -> {
                       try {
                           return rootValidate(timeout, ksa, event).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                       } catch (InterruptedException | TimeoutException e) {
                           throw new IllegalStateException(e);
                       } catch (ExecutionException e) {
                           throw new IllegalStateException(e.getCause());
                       }
                   });
    }

    private CompletableFuture<Boolean> rootValidate(Duration timeout, KeyStateWithEndorsementsAndValidations ksAttach,
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
        final var rootSet = roots.get();
        var last = ksAttach.validations()
                           .entrySet()
                           .stream()
                           .filter(e -> rootSet.contains(e.getKey()))
                           .map(e -> kerl.getKeyState(e.getKey()).thenApply(ks -> {
                               mapped.add(new resolved(ks, e.getValue()));
                               return ks;
                           }))
                           .reduce((a, b) -> a.thenCompose(ks -> b));

        if (last.isEmpty()) {
            log.trace("No mapped validations for {} on: {}", ksAttach.state().getCoordinates(), member.getId());
            return complete(SigningThreshold.thresholdMet(rootThreshold.get(), new int[] {}));
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
                                            signatures).verify(rootThreshold.get(), validations,
                                                               BbBackedInputStream.aggregate(event.toKeyEvent_()
                                                                                                  .toByteString()));
            return validated;
        }).exceptionally(t -> {
            log.error("Error in validating {} on: {}", ksAttach.state().getCoordinates(), member.getId(), t);
            return false;
        });
    }
}
