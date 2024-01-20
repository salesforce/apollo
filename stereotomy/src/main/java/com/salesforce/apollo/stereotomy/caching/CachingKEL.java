/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.stereotomy.caching;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.Verifier;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KEL;
import com.salesforce.apollo.stereotomy.KeyCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joou.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * A KEL that caches the state of the KEL with several caches
 * <ul>
 * <li>KeyState by event coordinate</li>
 * <li>Current KeyState by Identifier</li>
 * <li>KeyEvent by coordinates</li>
 * </ul>
 *
 * @author hal.hildebrand
 */
public class CachingKEL<K extends KEL.AppendKEL> implements KEL.AppendKEL {
    private static final Logger                                   log = LoggerFactory.getLogger(CachingKEL.class);
    private final        Function<Function<K, ?>, ?>              kelSupplier;
    private final        LoadingCache<EventCoordinates, KeyEvent> keyCoords;
    private final        LoadingCache<EventCoordinates, KeyState> ksCoords;

    public CachingKEL(Function<Function<K, ?>, ?> kelSupplier) {
        this(kelSupplier, defaultKsCoordsBuilder(null), defaultEventCoordsBuilder(null));
    }

    public CachingKEL(Function<Function<K, ?>, ?> kelSupplier, Caffeine<EventCoordinates, KeyState> builder,
                      Caffeine<EventCoordinates, KeyEvent> eventBuilder) {
        ksCoords = builder.build(new CacheLoader<>() {

            @Override
            public @Nullable KeyState load(EventCoordinates key) {
                return complete(kel -> kel.getKeyState(key));
            }
        });
        this.kelSupplier = kelSupplier;
        this.keyCoords = eventBuilder.build(new CacheLoader<>() {

            @Override
            public @Nullable KeyEvent load(EventCoordinates key) {
                return complete(kel -> kel.getKeyEvent(key));
            }
        });
    }

    public static Caffeine<EventCoordinates, KeyEvent> defaultEventCoordsBuilder(MetricsStatsCounter metrics) {
        var builder = Caffeine.newBuilder()
                              .maximumSize(10_000)
                              .expireAfterWrite(Duration.ofMinutes(10))
                              .removalListener((EventCoordinates coords, KeyEvent e, RemovalCause cause) -> log.trace(
                              "KeyEvent {} was removed ({})", coords, cause));
        if (metrics != null) {
            builder.recordStats(() -> metrics);
        }
        return builder;
    }

    public static Caffeine<EventCoordinates, KeyState> defaultKsCoordsBuilder(MetricsStatsCounter metrics) {
        var builder = Caffeine.newBuilder()
                              .maximumSize(10_000)
                              .expireAfterWrite(Duration.ofMinutes(10))
                              .removalListener((EventCoordinates coords, KeyState ks, RemovalCause cause) -> log.trace(
                              "KeyState {} was removed ({})", coords, cause));
        if (metrics != null) {
            builder.recordStats(() -> metrics);
        }
        return builder;
    }

    public KeyState append(KeyEvent event) {
        try {
            return complete(kel -> kel.append(event));
        } catch (Throwable e) {
            log.error("Cannot complete append", e);
            return null;
        } finally {
            keyCoords.invalidate(event.getCoordinates());
        }
    }

    @Override
    public List<KeyState> append(KeyEvent... events) {
        if (events == null || events.length == 0) {
            return Collections.emptyList();
        }
        try {
            return complete(kel -> kel.append(events));
        } catch (Throwable e) {
            log.error("Cannot complete append", e);
            return null;
        } finally {
            for (var event : events) {
                keyCoords.invalidate(event.getCoordinates());
            }
        }
    }

    @Override
    public List<KeyState> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        if (events.isEmpty() && attachments.isEmpty()) {
            return Collections.emptyList();
        }
        try {
            return complete(kel -> kel.append(events, attachments));
        } catch (Throwable e) {
            log.error("Cannot complete append", e);
            return null;
        }
    }

    public void clear() {
        keyCoords.invalidateAll();
        ksCoords.invalidateAll();
    }

    @Override
    public Attachment getAttachment(EventCoordinates coordinates) {
        try {
            return complete(kel -> kel.getAttachment(coordinates));
        } catch (Throwable e) {
            log.error("Cannot complete append", e);
            return null;
        }
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        try {
            return complete(KEL::getDigestAlgorithm);
        } catch (Throwable e) {
            log.error("Cannot complete append", e);
            return null;
        }
    }

    @Override
    public KeyEvent getKeyEvent(EventCoordinates coordinates) {
        return keyCoords.get(coordinates);
    }

    @Override
    public KeyState getKeyState(EventCoordinates coordinates) {
        return ksCoords.get(coordinates);
    }

    @Override
    public KeyState getKeyState(Identifier identifier) {
        try {
            return complete(kel -> kel.getKeyState(identifier));
        } catch (Throwable e) {
            log.error("Cannot complete append", e);
            return null;
        }
    }

    @Override
    public KeyState getKeyState(Identifier identifier, ULong sequenceNumber) {
        try {
            return complete(kel -> kel.getKeyState(identifier, sequenceNumber));
        } catch (Throwable e) {
            log.error("Cannot complete append", e);
            return null;
        }
    }

    @Override
    public KeyStateWithAttachments getKeyStateWithAttachments(EventCoordinates coordinates) {
        try {
            return complete(kel -> kel.getKeyStateWithAttachments(coordinates));
        } catch (Throwable e) {
            log.error("Cannot complete append", e);
            return null;
        }
    }

    @Override
    public Verifier.DefaultVerifier getVerifier(KeyCoordinates coordinates) {
        try {
            return complete(kel -> kel.getVerifier(coordinates));
        } catch (Throwable e) {
            log.error("Cannot complete append", e);
            return null;
        }
    }

    protected <T, I> T complete(Function<K, I> func) {
        try {
            @SuppressWarnings("unchecked")
            final var result = (T) kelSupplier.apply(func);
            return result;
        } catch (Throwable t) {
            log.error("Error completing cache", t);
            return null;
        }
    }
}
