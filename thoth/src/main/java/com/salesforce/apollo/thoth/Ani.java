/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import static com.salesforce.apollo.thoth.KerlDHT.completeIt;

import java.io.InputStream;
import java.security.PublicKey;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
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
import com.salesforce.apollo.comm.Router;
import com.salesforce.apollo.comm.Router.CommonCommunications;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.crypto.SigningThreshold;
import com.salesforce.apollo.crypto.Verifier.Filtered;
import com.salesforce.apollo.membership.Context;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.membership.SigningMember;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyStateImpl;
import com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.services.grpc.StereotomyMetrics;
import com.salesforce.apollo.thoth.grpc.ValidatorClient;
import com.salesforce.apollo.thoth.grpc.ValidatorServer;
import com.salesforce.apollo.thoth.grpc.ValidatorService;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * Key Event Validation
 *
 * @author hal.hildebrand
 *
 */
public class Ani {
    public record AniParameters(SigningMember member, Context<Member> context, SigningThreshold threshold,
                                Map<Identifier, Integer> validators, Duration validationTimeout, Sakshi sakshi,
                                Executor executor, KerlDHT dht, Router communications, StereotomyMetrics metrics) {}

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

    @SuppressWarnings("unused")
    private final CommonCommunications<ValidatorService, Sakshi> comms;
    @SuppressWarnings("unused")
    private final Context<Member>                                context;
    private final KerlDHT                                        dht;
    private final AsyncLoadingCache<EventCoordinates, KeyEvent>  events;
    private final AsyncLoadingCache<Identifier, KeyState>        keyStates;
    private final SigningMember                                  member;
    private final Sakshi                                         sakshi;
    private final SigningThreshold                               threshold;
    private final AsyncLoadingCache<EventCoordinates, Boolean>   validated;
    private final Map<Identifier, Integer>                       validators;

    public Ani(AniParameters parameters) {
        this(parameters, defaultValidatedBuilder(), defaultEventsBuilder(), defaultKeyStatesBuilder());
    }

    public Ani(AniParameters parameters, Caffeine<EventCoordinates, Boolean> validatedBuilder,
               Caffeine<EventCoordinates, KeyEvent> eventsBuilder, Caffeine<Identifier, KeyState> keyStatesBuilder) {
        validated = validatedBuilder.buildAsync(new AsyncCacheLoader<>() {
            @Override
            public CompletableFuture<? extends Boolean> asyncLoad(EventCoordinates key,
                                                                  Executor executor) throws Exception {
                return performValidation(key, parameters.validationTimeout);
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
        this.member = parameters.member;
        this.dht = parameters.dht;
        this.validators = parameters.validators;
        this.threshold = parameters.threshold;
        this.sakshi = parameters.sakshi;
        comms = parameters.communications.create(member, parameters.context.getId(), this.sakshi,
                                                 r -> new ValidatorServer(r, parameters.executor, parameters.metrics),
                                                 ValidatorClient.getCreate(parameters.context.getId(),
                                                                           parameters.metrics),
                                                 ValidatorClient.getLocalLoopback(this.sakshi, member));
        this.context = parameters.context;
    }

    public EventValidation eventValidation(Duration timeout) {
        return new EventValidation() {
            @Override
            public Filtered filtered(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                                     InputStream message) {
                return null; // TODO
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
                return true; // TODO - lol
            }

            @Override
            public boolean verify(EventCoordinates coordinates, JohnHancock signature, InputStream message) {
                return true; // TODO
            }

            @Override
            public boolean verify(EventCoordinates coordinates, SigningThreshold threshold, JohnHancock signature,
                                  InputStream message) {
                // TODO Auto-generated method stub
                return false;
            }
        };
    }

    public CompletableFuture<Boolean> validate(KeyEvent event) {
        events.put(event.getCoordinates(), completeIt(event));
        return validated.get(event.getCoordinates());
    }

    private CompletableFuture<? extends Boolean> performValidation(EventCoordinates coord, Duration timeout) {
        var fs = new CompletableFuture<Boolean>();
        events.get(coord)
              .thenAcceptBoth(dht.getKeyStateWithEndorsementsAndValidations(coord.toEventCoords()),
                              (event, ksa) -> fs.complete(validate(timeout, ksa, event)));
        return fs;
    }

    private boolean validate(Duration timeout, KeyStateWithEndorsementsAndValidations ksAttach, KeyEvent event) {
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

            record resolved(Entry<Identifier, Integer> entry, KeyState keyState) {}
            validators.entrySet()
                      .stream()
                      .map(e -> keyStates.get(e.getKey())
                                         .orTimeout(timeout.toNanos(), TimeUnit.NANOSECONDS)
                                         .exceptionallyCompose(t -> null)
                                         .thenApply(ks -> new resolved(e, ks)))
                      .map(res -> res.join())
                      .filter(res -> res != null)
                      .forEach(res -> validations[res.entry.getValue()] = res.keyState.getKeys().get(0));
            validated = new JohnHancock(algo, signatures).verify(threshold, validations,
                                                                 BbBackedInputStream.aggregate(event.toKeyEvent_()
                                                                                                    .toByteString()));
        }
        return (witnessed && validated);
    }
}
