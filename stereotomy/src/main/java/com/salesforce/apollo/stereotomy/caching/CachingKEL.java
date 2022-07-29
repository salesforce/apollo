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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.CacheLoader;
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
                                         RemovalCause cause) -> log.trace("KeyEvent %s was removed ({}){}", coords,
                                                                          cause));
    }

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

    private final Function<Function<K, ?>, ?>                   kelSupplier;
    private final AsyncLoadingCache<EventCoordinates, KeyEvent> keyCoords;
    private final AsyncLoadingCache<EventCoordinates, KeyState> ksCoords;
    private final AsyncLoadingCache<Identifier, KeyState>       ksCurrent;

    public CachingKEL(Function<Function<K, ?>, ?> kelSupplier) {
        this(kelSupplier, defaultKsCoordsBuilder(), defaultKsCurrentBuilder(), defaultEventCoordsBuilder());
    }

    public CachingKEL(Function<Function<K, ?>, ?> kelSupplier, Caffeine<EventCoordinates, KeyState> builder,
                      Caffeine<Identifier, KeyState> curBuilder, Caffeine<EventCoordinates, KeyEvent> eventBuilder) {
        ksCoords = builder.buildAsync(CacheLoader.bulk(coords -> load(coords)));
        ksCurrent = curBuilder.buildAsync(CacheLoader.bulk(ids -> loadCurrent(ids)));
        this.kelSupplier = kelSupplier;
        this.keyCoords = eventBuilder.buildAsync(AsyncCacheLoader.bulk(coords -> loadEvents(coords)));
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        return complete(kel -> {
            final var fs = kel.append(event);
            fs.whenComplete((ks, t) -> {
                if (t != null) {
                    ksCurrent.synchronous().invalidate(event.getIdentifier());
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
                    ksCurrent.synchronous().invalidate(event[0].getIdentifier());
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
                        ksCurrent.synchronous().invalidate(events.get(0).getIdentifier());
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
    public CompletableFuture<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return complete(kel -> keyCoords.get(coordinates));
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(EventCoordinates coordinates) {
        return complete(kel -> ksCoords.get(coordinates));
    }

    @Override
    public CompletableFuture<KeyState> getKeyState(Identifier identifier) {
        return complete(kel -> ksCurrent.get(identifier));
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
            CompletableFuture<KeyState> ks = null;
            for (var c : coords) {
                if (ks == null) {
                    ks = kel.getKeyState(c);
                } else {
                    ks = ks.thenCompose(ke -> kel.getKeyState(c));
                }
                ks.thenApply(state -> {
                    loaded.put(c, state);
                    return state;
                });
            }
            try {
                return ks.thenApply(ke -> loaded).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (ExecutionException e) {
                log.trace("Unable to load key state for coords: {} ", coords, e);
            }
            return loaded;
        });
    }

    private Map<Identifier, KeyState> loadCurrent(Set<? extends Identifier> ids) {
        var loaded = new HashMap<Identifier, KeyState>();
        return complete(kel -> {
            CompletableFuture<KeyState> ks = null;
            for (var id : ids) {
                if (ks == null) {
                    ks = kel.getKeyState(id);
                } else {
                    ks = ks.thenCompose(ke -> kel.getKeyState(id));
                }
                ks.thenApply(state -> {
                    loaded.put(id, state);
                    return state;
                });
            }
            try {
                return ks.thenApply(ke -> loaded).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (ExecutionException e) {
                log.trace("Unable to load key state for ids: {} ", ids, e);
            }
            return loaded;
        });
    }

    private Map<EventCoordinates, KeyEvent> loadEvents(Set<? extends EventCoordinates> coords) {
        var loaded = new HashMap<EventCoordinates, KeyEvent>();
        return complete(kel -> {
            CompletableFuture<KeyEvent> ks = null;
            for (var c : coords) {
                if (ks == null) {
                    ks = kel.getKeyEvent(c);
                } else {
                    ks = ks.thenCompose(ke -> kel.getKeyEvent(c));
                }
                ks.thenApply(state -> {
                    loaded.put(c, state);
                    return state;
                });
            }
            try {
                return ks.thenApply(ke -> loaded).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                log.trace("Unable to load events for coordinates: {} ", coords, e);
            }
            return loaded;
        });
    }
}
