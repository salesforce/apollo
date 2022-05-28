/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import java.security.PublicKey;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
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
public class Ani implements EventValidation {
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

    private final KerlDHT                                      dht;
    private final LoadingCache<EventCoordinates, KeyEvent>     events;
    private final AsyncLoadingCache<Identifier, KeyState>      keyStates;
    private final SigningThreshold                             threshold;
    private final AsyncLoadingCache<EventCoordinates, Boolean> validated;
    private final Map<Identifier, Integer>                     validators;
    private final Duration                                     timeout = Duration.ofSeconds(60);

    public Ani(List<? extends Identifier> validators, SigningThreshold threshold, KerlDHT dht) {
        this(validators, threshold, dht, defaultValidatedBuilder(), defaultEventsBuilder(), defaultKeyStatesBuilder());
    }

    public Ani(List<? extends Identifier> validators, SigningThreshold threshold, KerlDHT dht,
               Caffeine<EventCoordinates, Boolean> validatedBuilder, Caffeine<EventCoordinates, KeyEvent> eventsBuilder,
               Caffeine<Identifier, KeyState> keyStatesBuilder) {
        validated = validatedBuilder.buildAsync(AsyncCacheLoader.bulk(coords -> validateCoords(coords)));
        events = eventsBuilder.build(CacheLoader.bulk(coords -> loadEvents(coords)));
        keyStates = keyStatesBuilder.buildAsync(AsyncCacheLoader.bulk(coords -> loadKeyStates(coords)));
        this.dht = dht;
        this.validators = new HashMap<>();
        for (int i = 0; i < validators.size(); i++) {
            this.validators.put(validators.get(i), i);
        }
        this.threshold = threshold;
    }

    @Override
    public boolean validate(EstablishmentEvent event) {
        events.put(event.getCoordinates(), event);
        try {
            return validated.get(event.getCoordinates()).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException(e);
        }
    }

    private Map<EventCoordinates, KeyEvent> loadEvents(Set<? extends EventCoordinates> coords) {
        var loaded = new HashMap<EventCoordinates, KeyEvent>();
        for (var coord : coords) {
            try {
                loaded.put(coord, ProtobufEventFactory.from(dht.getKeyEvent(coord.toEventCoords())
                                                               .get(timeout.toMillis(), TimeUnit.MILLISECONDS)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Collections.emptyMap();
            } catch (ExecutionException e) {
                log.error("Cannot load key event: {}", coords, e);
                continue;
            } catch (TimeoutException e) {
                log.error("Cannot load key event: {}", coords, e);
            }
        }
        return loaded;
    }

    private Map<Identifier, KeyState> loadKeyStates(Set<? extends Identifier> identifier) {
        var loaded = new HashMap<Identifier, KeyState>();
        for (var id : identifier) {
            try {
                loaded.put(id, new KeyStateImpl(dht.getKeyState(id.toIdent())
                                                   .get(timeout.toMillis(), TimeUnit.MILLISECONDS)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Collections.emptyMap();
            } catch (ExecutionException e) {
                log.error("Cannot load key event: {}", id, e);
                continue;
            } catch (TimeoutException e) {
                log.error("Cannot load key event: {}", id, e);
            }
        }
        return loaded;
    }

    private boolean performValidation(EventCoordinates coord) {
        final var ke = events.get(coord);
        if (ke instanceof EstablishmentEvent event) {
            return performValidation(event);
        }
        return false;
    }

    private boolean performValidation(KeyEvent event) {
        KeyStateWithEndorsementsAndValidations ksAttach;
        try {
            ksAttach = dht.getKeyStateWithEndorsementsAndValidations(event.getCoordinates().toEventCoords())
                          .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        } catch (TimeoutException e) {
            throw new IllegalStateException(e);
        }
        return validate(ksAttach, event);
    }

    private boolean validate(KeyStateWithEndorsementsAndValidations ksAttach, KeyEvent event) {
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
        return witnessed && validated;
    }

    private Map<EventCoordinates, Boolean> validateCoords(Set<? extends EventCoordinates> coords) {
        Map<EventCoordinates, Boolean> valid = new HashMap<>();
        for (EventCoordinates coord : coords) {
            valid.put(coord, performValidation(coord));
        }
        return valid;
    }
}
