/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import static com.salesforce.apollo.thoth.KerlDHT.completeIt;

import java.security.PublicKey;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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
import com.salesfoce.apollo.thoth.proto.KeyStateWithEndorsementsAndValidations;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
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

    public static Caffeine<EventCoordinates, KeyEvent> defaultEventsBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((EventCoordinates coords, KeyEvent event,
                                         RemovalCause cause) -> log.trace("Event %s was removed ({}){}", coords,
                                                                          cause));
    }

    public static Caffeine<Identifier, KeyState> defaultKeyStatesBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((Identifier coords, KeyState ks,
                                         RemovalCause cause) -> log.trace("Key state %s was removed ({}){}", coords,
                                                                          cause));
    }

    public static Caffeine<EventCoordinates, Boolean> defaultValidatedBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((EventCoordinates coords, Boolean validated,
                                         RemovalCause cause) -> log.trace("Validation %s was removed ({}){}", coords,
                                                                          cause));
    }

    private final KerlDHT                                       dht;
    private final AsyncLoadingCache<EventCoordinates, KeyEvent> events;
    private final AsyncLoadingCache<Identifier, KeyState>       keyStates;
    private final SigningThreshold                              threshold;
    private final Duration                                      timeout = Duration.ofSeconds(60);
    private final AsyncLoadingCache<EventCoordinates, Boolean>  validated;
    private final Map<Identifier, Integer>                      validators;

    public Ani(List<? extends Identifier> validators, SigningThreshold threshold, KerlDHT dht) {
        this(validators, threshold, dht, defaultValidatedBuilder(), defaultEventsBuilder(), defaultKeyStatesBuilder());
    }

    public Ani(List<? extends Identifier> validators, SigningThreshold threshold, KerlDHT dht,
               Caffeine<EventCoordinates, Boolean> validatedBuilder, Caffeine<EventCoordinates, KeyEvent> eventsBuilder,
               Caffeine<Identifier, KeyState> keyStatesBuilder) {
        validated = validatedBuilder.buildAsync(new AsyncCacheLoader<>() {
            @Override
            public CompletableFuture<? extends Boolean> asyncLoad(EventCoordinates key,
                                                                  Executor executor) throws Exception {
                return performValidation(key);
            }
        });
        events = eventsBuilder.buildAsync(new AsyncCacheLoader<>() {
            @Override
            public CompletableFuture<? extends KeyEvent> asyncLoad(EventCoordinates key,
                                                                   Executor executor) throws Exception {
                return dht.getKeyEvent(key.toEventCoords()).thenApply(ke -> ProtobufEventFactory.from(ke));
            }
        });
        keyStates = keyStatesBuilder.buildAsync(new AsyncCacheLoader<>() {
            @Override
            public CompletableFuture<? extends KeyState> asyncLoad(Identifier id, Executor executor) throws Exception {
                return dht.getKeyState(id.toIdent()).thenApply(ks -> new KeyStateImpl(ks));
            }
        });
        this.dht = dht;
        this.validators = new HashMap<>();
        for (int i = 0; i < validators.size(); i++) {
            this.validators.put(validators.get(i), i);
        }
        this.threshold = threshold;
    }

    public EventValidation getValidation(Duration timeout) {
        return new EventValidation() {
            @Override
            public boolean validate(EstablishmentEvent event) {
                try {
                    return Ani.this.validate(event).get(timeout.toNanos(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                } catch (ExecutionException e) {
                    log.error("Unable to validate: " + event.getCoordinates());
                    return false;
                } catch (TimeoutException e) {
                    log.error("Timeout validating: " + event.getCoordinates());
                    return false;
                }
            }
        };
    }

    public CompletableFuture<Boolean> validate(KeyEvent event) {
        events.put(event.getCoordinates(), completeIt(event));
        return validated.get(event.getCoordinates());
    }

    private CompletableFuture<? extends Boolean> performValidation(EventCoordinates coord) {
        var fs = new CompletableFuture<Boolean>();
        events.get(coord)
              .thenAcceptBoth(dht.getKeyStateWithEndorsementsAndValidations(coord.toEventCoords()),
                              (event, ksa) -> validate(ksa, event).whenComplete((b, t) -> {
                                  if (t != null) {
                                      fs.completeExceptionally(t);
                                  } else {
                                      fs.complete(b);
                                  }
                              }));
        return fs;
    }

    private CompletableFuture<Boolean> validate(KeyStateWithEndorsementsAndValidations ksAttach, KeyEvent event) {
        // TODO Multisig
        var state = new KeyStateImpl(ksAttach.getState());
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
            if (!ksAttach.getEndorsementsMap().isEmpty()) {
                for (var entry : ksAttach.getEndorsementsMap().entrySet()) {
                    signatures[entry.getKey()] = JohnHancock.from(entry.getValue()).getBytes()[0];
                }
            }
            witnessed = new JohnHancock(algo, signatures).verify(state.getSigningThreshold(), witnesses,
                                                                 BbBackedInputStream.aggregate(event.toKeyEvent_()
                                                                                                    .toByteString()));
        }
        boolean validated = false;
        if (validators.isEmpty()) {
            validated = true;
        } else {
            var signatures = new byte[validators.size()][];
            if (!ksAttach.getEndorsementsMap().isEmpty()) {
                for (var entry : ksAttach.getValidationsList()) {
                    Integer index = validators.get(Identifier.from(entry.getValidator()));
                    if (index == null) {
                        continue;
                    }
                    signatures[index] = JohnHancock.from(entry.getSignature()).getBytes()[0];
                }
            }
            var validations = new PublicKey[state.getWitnesses().size()];
            SignatureAlgorithm algo = SignatureAlgorithm.lookup(validations[0]);
            for (var entry : validators.entrySet()) {
                try {
                    validations[entry.getValue()] = keyStates.get(entry.getKey())
                                                             .get(timeout.toMillis(), TimeUnit.MILLISECONDS)
                                                             .getKeys()
                                                             .get(0);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    continue;
                } catch (ExecutionException e) {
                    log.error("Error retreiving key state: {}", entry.getKey(), e);
                } catch (TimeoutException e) {
                    log.error("Error retreiving key state: {}", entry.getKey(), e);
                }
            }
            validated = new JohnHancock(algo, signatures).verify(threshold, validations,
                                                                 BbBackedInputStream.aggregate(event.toKeyEvent_()
                                                                                                    .toByteString()));
        }
        return completeIt(witnessed && validated);
    }
}
