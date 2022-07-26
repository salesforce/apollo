/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.stereotomy.caching;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.salesforce.apollo.crypto.Digest;
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

    public static Caffeine<EventCoordinates, KeyState> defaultKsCoordsBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((EventCoordinates coords, KeyState ks,
                                         RemovalCause cause) -> log.trace("KeyState %s was removed ({}){}", coords,
                                                                          cause));
    }

    public static Caffeine<Identifier, KeyState> defaultKsCurrentBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((Identifier id, KeyState ks,
                                         RemovalCause cause) -> log.trace("Current KeyState %s was removed ({}){}", id,
                                                                          cause));
    }

    private final Function<Function<K, ?>, ?>              kelSupplier;
    private final LoadingCache<EventCoordinates, KeyState> ksCoords;
    private final LoadingCache<Identifier, KeyState>       ksCurrent;

    public CachingKEL(Function<Function<K, ?>, ?> kelSupplier) {
        this(kelSupplier, defaultKsCoordsBuilder(), defaultKsCurrentBuilder());
    }

    public CachingKEL(Function<Function<K, ?>, ?> kelSupplier, Caffeine<EventCoordinates, KeyState> builder,
                      Caffeine<Identifier, KeyState> curBuilder) {
        ksCoords = builder.build(CacheLoader.bulk(coords -> load(coords)));
        ksCurrent = curBuilder.build(CacheLoader.bulk(ids -> loadCurrent(ids)));
        this.kelSupplier = kelSupplier;
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        return complete(kel -> {
            final var fs = kel.append(event);
            fs.whenComplete((ks, t) -> {
                if (t != null) {
                    ksCurrent.invalidate(event.getIdentifier());
                }
            });
            return fs;
        });
    }

    @Override
    public CompletableFuture<List<KeyState>> append(KeyEvent... event) {
        if (event == null || event.length == 0) {
            var fs = new CompletableFuture<List<KeyState>>();
            fs.complete(Collections.emptyList());
            return fs;
        }
        return complete(kel -> {
            final var fs = kel.append(event);
            fs.whenComplete((ks, t) -> {
                if (t != null) {
                    ksCurrent.invalidate(event[0].getIdentifier());
                }
            });
            return fs;
        });
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        if (events.isEmpty() && attachments.isEmpty()) {
            var fs = new CompletableFuture<List<KeyState>>();
            fs.complete(Collections.emptyList());
            return fs;
        }
        return complete(kel -> {
            final var fs = kel.append(events, attachments);
            if (!events.isEmpty()) {
                fs.whenComplete((ks, t) -> {
                    if (t != null) {
                        ksCurrent.invalidate(events.get(0).getIdentifier());
                    }
                });
            }
            return fs;
        });
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
    public CompletableFuture<KeyEvent> getKeyEvent(Digest digest) {
        return complete(kel -> kel.getKeyEvent(digest));
    }

    @Override
    public CompletableFuture<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return complete(kel -> kel.getKeyEvent(coordinates));
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(EventCoordinates coordinates) {
        return complete(kel -> Optional.ofNullable(ksCoords.get(coordinates)));
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(Identifier identifier) {
        return complete(kel -> Optional.ofNullable(ksCurrent.get(identifier)));
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

    private Map<EventCoordinates, KeyState> load(Set<? extends EventCoordinates> coords) {
        var loaded = new HashMap<EventCoordinates, KeyState>();
        return complete(kel -> {
            coords.forEach(c -> {
                var ks = kel.getKeyState(c);
                ks.whenComplete((state, t) -> {
                    if (t != null) {
                        loaded.put(c, state);
                    }
                });
            });
            return loaded;
        });
    }

    private Map<Identifier, KeyState> loadCurrent(Set<? extends Identifier> ids) {
        var loaded = new HashMap<Identifier, KeyState>();
        return complete(kel -> {
            ids.forEach(id -> {
                var ks = kel.getKeyState(id);
                ks.whenComplete((state, t) -> {
                    if (t != null) {
                        loaded.put(id, state);
                    }
                });
            });
            return loaded;
        });
    }
}
