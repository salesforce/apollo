/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.stereotomy.caching;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.Verifier;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KEL;
import com.salesforce.apollo.stereotomy.KeyCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * A KEL that caches the state of the KEL with several caches
 * <ul>
 * <li>KeyState by event coordinate</li>
 * <li>Current KeyState by Identifier</li>
 * <li>KeyEvent by coordinates</li>
 * </ul>
 * 
 * 
 * @author hal.hildebrand
 *
 */
public class CachingKEL<K extends KEL> implements KEL {
    private static final Logger log = LoggerFactory.getLogger(CachingKEL.class);

    public static Caffeine<EventCoordinates, KeyEvent> defaultEventCoordsBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((EventCoordinates coords, KeyEvent e,
                                         RemovalCause cause) -> log.trace("KeyEvent {} was removed ({})", coords,
                                                                          cause));
    }

    public static Caffeine<EventCoordinates, KeyState> defaultKsCoordsBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((EventCoordinates coords, KeyState ks,
                                         RemovalCause cause) -> log.trace("KeyState {} was removed ({})", coords,
                                                                          cause));
    }

    private final Function<Function<K, ?>, ?>                   kelSupplier;
    private final AsyncLoadingCache<EventCoordinates, KeyEvent> keyCoords;
    private final AsyncLoadingCache<EventCoordinates, KeyState> ksCoords;

    public CachingKEL(Function<Function<K, ?>, ?> kelSupplier) {
        this(kelSupplier, defaultKsCoordsBuilder(), defaultEventCoordsBuilder());
    }

    public CachingKEL(Function<Function<K, ?>, ?> kelSupplier, Caffeine<EventCoordinates, KeyState> builder,
                      Caffeine<EventCoordinates, KeyEvent> eventBuilder) {
        ksCoords = builder.buildAsync(new AsyncCacheLoader<>() {

            @Override
            public CompletableFuture<? extends KeyState> asyncLoad(EventCoordinates key,
                                                                   Executor executor) throws Exception {
                return complete(kel -> kel.getKeyState(key));
            }

            @Override
            public CompletableFuture<? extends Map<? extends EventCoordinates, ? extends KeyState>> asyncLoadAll(Set<? extends EventCoordinates> keys,
                                                                                                                 Executor executor) throws Exception {
                return AsyncCacheLoader.super.asyncLoadAll(keys, executor);
            }

            @Override
            public CompletableFuture<? extends KeyState> asyncReload(EventCoordinates key, KeyState oldValue,
                                                                     Executor executor) throws Exception {
                return AsyncCacheLoader.super.asyncReload(key, oldValue, executor);
            }
        });
        this.kelSupplier = kelSupplier;
        this.keyCoords = eventBuilder.buildAsync(new AsyncCacheLoader<>() {

            @Override
            public CompletableFuture<? extends KeyEvent> asyncLoad(EventCoordinates key,
                                                                   Executor executor) throws Exception {
                return complete(kel -> kel.getKeyEvent(key));
            }

            @Override
            public CompletableFuture<? extends Map<? extends EventCoordinates, ? extends KeyEvent>> asyncLoadAll(Set<? extends EventCoordinates> keys,
                                                                                                                 Executor executor) throws Exception {
                return AsyncCacheLoader.super.asyncLoadAll(keys, executor);
            }

            @Override
            public CompletableFuture<? extends KeyEvent> asyncReload(EventCoordinates key, KeyEvent oldValue,
                                                                     Executor executor) throws Exception {
                return AsyncCacheLoader.super.asyncReload(key, oldValue, executor);
            }
        });
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        try {
            return complete(kel -> kel.append(event));
        } finally {
            keyCoords.synchronous().invalidate(event.getCoordinates());
        }
    }

    @Override
    public CompletableFuture<List<KeyState>> append(KeyEvent... events) {
        if (events == null || events.length == 0) {
            var fs = new CompletableFuture<List<KeyState>>();
            fs.complete(Collections.emptyList());
            return fs;
        }
        try {
            return complete(kel -> kel.append(events));
        } finally {
            for (var event : events) {
                keyCoords.synchronous().invalidate(event.getCoordinates());
            }
        }
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        if (events.isEmpty() && attachments.isEmpty()) {
            var fs = new CompletableFuture<List<KeyState>>();
            fs.complete(Collections.emptyList());
            return fs;
        }
        return complete(kel -> kel.append(events, attachments));
    }

    @Override
    public CompletableFuture<Attachment> getAttachment(EventCoordinates coordinates) {
        return complete(kel -> kel.getAttachment(coordinates));
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return complete(kel -> kel.getDigestAlgorithm());
    }

    @Override
    public CompletableFuture<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return keyCoords.get(coordinates);
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(EventCoordinates coordinates) {
        return ksCoords.get(coordinates);
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(Identifier identifier) {
        return complete(kel -> kel.getKeyState(identifier));
    }

    @Override
    public CompletableFuture<KeyStateWithAttachments> getKeyStateWithAttachments(EventCoordinates coordinates) {
        return complete(kel -> kel.getKeyStateWithAttachments(coordinates));
    }

    @Override
    public CompletableFuture<Verifier> getVerifier(KeyCoordinates coordinates) {
        return complete(kel -> kel.getVerifier(coordinates));
    }

    protected <T, I> T complete(Function<K, I> func) {
        @SuppressWarnings("unchecked")
        final var result = (T) kelSupplier.apply(func);
        return result;
    }
}
