/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import java.security.PublicKey;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.SignatureAlgorithm;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.EventValidation;
import com.salesforce.apollo.stereotomy.KEL.KeyStateWithAttachments;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.event.EstablishmentEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.utils.BbBackedInputStream;

/**
 * Key Event Validation
 *
 * @author hal.hildebrand
 *
 */
public class Ani implements EventValidation {
    private static final Logger log = LoggerFactory.getLogger(Ani.class);

    private static Caffeine<EventCoordinates, KeyEvent> defaultEventsBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((EventCoordinates coords, KeyEvent event,
                                         RemovalCause cause) -> log.trace("Event %s was removed ({}){}", coords,
                                                                          cause));
    }

    private static Caffeine<EventCoordinates, Boolean> defaultValidatedBuilder() {
        return Caffeine.newBuilder()
                       .maximumSize(10_000)
                       .expireAfterWrite(Duration.ofMinutes(10))
                       .removalListener((EventCoordinates coords, Boolean validated,
                                         RemovalCause cause) -> log.trace("Validation %s was removed ({}){}", coords,
                                                                          cause));
    }

    private final LoadingCache<EventCoordinates, KeyEvent> events;
    private final KERL                                     kerl;
    private final LoadingCache<EventCoordinates, Boolean>  validated;

    public Ani(KERL kerl) {
        this(kerl, defaultValidatedBuilder(), defaultEventsBuilder());
    }

    public Ani(KERL kerl, Caffeine<EventCoordinates, Boolean> validatedBuilder,
               Caffeine<EventCoordinates, KeyEvent> eventsBuilder) {
        validated = validatedBuilder.build(CacheLoader.bulk(coords -> loadValidated(coords)));
        events = eventsBuilder.build(CacheLoader.bulk(coords -> loadEvents(coords)));
        this.kerl = kerl;
    }

    @Override
    public boolean validate(EstablishmentEvent event) {
        events.put(event.getCoordinates(), event);
        return validated.get(event.getCoordinates());
    }

    private Map<EventCoordinates, KeyEvent> loadEvents(Set<? extends EventCoordinates> coords) {
        var loaded = new HashMap<EventCoordinates, KeyEvent>();
        for (var coord : coords) {
            var ke = kerl.getKeyEvent(coord);
            if (!ke.isEmpty()) {
                loaded.put(coord, ke.get());
            }
        }
        return loaded;
    }

    private Map<EventCoordinates, Boolean> loadValidated(Set<? extends EventCoordinates> coords) {
        Map<EventCoordinates, Boolean> valid = new HashMap<>();
        for (EventCoordinates coord : coords) {
            performValidation(coord);
        }
        return valid;
    }

    private boolean performValidation(EstablishmentEvent event) {
        var ksAttach = kerl.getKeyStateWithAttachments(event.getCoordinates());
        if (ksAttach.isEmpty()) {
            return false;
        }
        return validate(ksAttach.get(), event);
    }

    private boolean performValidation(EventCoordinates coord) {
        final var ke = events.get(coord);
        if (ke instanceof EstablishmentEvent event) {
            return performValidation(event);
        }
        return false;
    }

    private boolean validate(KeyStateWithAttachments ksa, EstablishmentEvent event) {
        var state = ksa.state();
        if (state.getWitnesses().isEmpty()) {
            return true; // TODO - ? HSH
        }
        var witnesses = new PublicKey[state.getWitnesses().size()];
        for (var i = 0; i < state.getWitnesses().size(); i++) {
            witnesses[i] = state.getWitnesses().get(i).getPublicKey();
        }
        var algo = SignatureAlgorithm.lookup(witnesses[0]);
        byte[][] signatures = new byte[state.getWitnesses().size()][];
        if (ksa.attachments() != null) {
            for (var entry : ksa.attachments().endorsements().entrySet()) {
                signatures[entry.getKey()] = entry.getValue().getBytes()[0]; // TODO - HSH
            }
        }
        JohnHancock combined = new JohnHancock(algo, signatures);
        return combined.verify(event.getSigningThreshold(), witnesses,
                               BbBackedInputStream.aggregate(event.toKeyEvent_().toByteString()));
    }
}
