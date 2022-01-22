/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.mem;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.processing.KeyEventProcessor;

/**
 * @author hal.hildebrand
 *
 */
public class MemKERL implements KERL {

    /**
     * Ordering by
     * 
     * <pre>
     * <coords.identifier, coords.sequenceNumber, coords.digest>
     * </pre>
     */
    public static String coordinateOrdering(EventCoordinates coords) {
        return qb64(coords.getIdentifier()) + ':' + coords.getSequenceNumber() + ':' + qb64(coords.getDigest());
    }

    public static String receiptDigestSuffix(EventCoordinates event, EventCoordinates signer) {
        return qb64(event.getDigest()) + ':' + qb64(signer.getDigest());
    }

    /**
     * Ordering by
     * 
     * <pre>
     * <event.identifier, signer.identifier, event.sequenceNumber, signer.sequenceNumber, event.digest, signer.digest>
     * </pre>
     */
    public static String receiptOrdering(EventCoordinates event, EventCoordinates signer) {
        return receiptPrefix(event, signer) + receiptSequence(event, signer) + receiptDigestSuffix(event, signer);
    }

    public static String receiptPrefix(EventCoordinates event, EventCoordinates signer) {
        return receiptPrefix(event.getIdentifier(), signer.getIdentifier());
    }

    public static String receiptPrefix(Identifier forIdentifier, Identifier forIdentifier2) {
        return qb64(forIdentifier) + ':' + qb64(forIdentifier2) + '.';
    }

    public static String receiptSequence(EventCoordinates event, EventCoordinates signer) {
        return event.getSequenceNumber().toString() + ':' + signer.getSequenceNumber() + '.';
    }

    private final DigestAlgorithm digestAlgorithm;
    // Order by <stateOrdering>
    private final Map<String, KeyEvent> events       = new ConcurrentHashMap<>();
    private final Map<Digest, String>   eventsByHash = new ConcurrentHashMap<>();

    // Order by <stateOrdering>
    private final Map<String, KeyState> keyState = new ConcurrentHashMap<>();

    // Order by <identifier>
    private final Map<String, String> keyStateByIdentifier = new ConcurrentHashMap<>();

    private final Map<String, Digest> locationToHash = new ConcurrentHashMap<>();

    private final KeyEventProcessor processor = new KeyEventProcessor(this);

    // Order by <receiptOrdering>
    private final Map<String, AttachmentEvent.Attachment> receipts = new ConcurrentHashMap<>();

    public MemKERL(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

    @Override
    public CompletableFuture<Void> append(AttachmentEvent event) {
        appendAttachments(event.coordinates(), event.attachments());
        var returned = new CompletableFuture<Void>();
        returned.complete(null);
        return returned;
    }

    @Override
    public CompletableFuture<KeyState> append(KeyEvent event) {
        final var newState = processor.process(event);
        append(event, newState);
        var f = new CompletableFuture<KeyState>();
        f.complete(newState);
        return f;
    }

    @Override
    public CompletableFuture<List<KeyState>> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        var states = events.stream().map(event -> {
            try {
                return append(event).get();
            } catch (InterruptedException | ExecutionException e) {
                return null;
            }
        }).toList();
        attachments.forEach(attach -> {
            try {
                append(attach).get();
            } catch (InterruptedException | ExecutionException e) {
            }
        });
        var fs = new CompletableFuture<List<KeyState>>();
        fs.complete(states);
        return fs;
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(Digest digest) {
        String coordinates = eventsByHash.get(digest);
        return coordinates == null ? Optional.empty() : Optional.of(events.get(coordinates));
    }

    @Override
    public Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return Optional.ofNullable(events.get(coordinateOrdering(coordinates)));
    }

    @Override
    public Optional<KeyState> getKeyState(EventCoordinates coordinates) {
        return Optional.ofNullable(keyState.get(coordinateOrdering(coordinates)));
    }

    @Override
    public Optional<KeyState> getKeyState(Identifier identifier) {
        String stateHash = keyStateByIdentifier.get(qb64(identifier));

        return stateHash == null ? Optional.empty() : Optional.ofNullable(keyState.get(stateHash));
    }

    @Override
    public Optional<List<EventWithAttachments>> kerl(Identifier identifier) {
        var current = getKeyState(identifier);
        if (current.isEmpty()) {
            return Optional.empty();
        }
        var coordinates = current.get().getCoordinates();
        var keyEvent = getKeyEvent(coordinates);
        if (keyEvent.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(kerl(keyEvent.get()));
    }

    private List<EventWithAttachments> kerl(KeyEvent event) {
        var current = event;
        var result = new ArrayList<EventWithAttachments>();
        while (current != null) {
            var coordinates = current.getCoordinates();
            result.add(new EventWithAttachments(current, getAttachment(coordinates).orElse(null)));
            current = getKeyEvent(current.getPrevious()).orElse(null);
        }
        Collections.reverse(result);
        return result;
    }

    private void append(KeyEvent event, KeyState newState) {
        String coordinates = coordinateOrdering(event.getCoordinates());
        events.put(coordinates, event);
        eventsByHash.put(newState.getDigest(), coordinates);
        locationToHash.put(coordinates, newState.getDigest());
        keyState.put(coordinates, newState);
        keyStateByIdentifier.put(qb64(event.getIdentifier()), coordinates);
    }

    private void appendAttachments(EventCoordinates coordinates, AttachmentEvent.Attachment attachment) {
        var key = coordinateOrdering(coordinates);
        var previous = receipts.get(key);
        receipts.put(key, combine(attachment, previous));
    }

    private Attachment combine(Attachment attachment, Attachment previous) {
        if (previous == null) {
            return attachment;
        }
        List<Seal> seals = new ArrayList<>(previous.seals());
        seals.addAll(attachment.seals());
        Map<Integer, JohnHancock> endorsements = new HashMap<>(previous.endorsements());
        endorsements.putAll(attachment.endorsements());

        return new AttachmentEvent.Attachment() {

            @Override
            public Map<Integer, JohnHancock> endorsements() {
                return endorsements;
            }

            @Override
            public List<Seal> seals() {
                return seals;
            }
        };
    }

    @Override
    public Optional<Attachment> getAttachment(EventCoordinates coordinates) {
        return Optional.ofNullable(receipts.get(coordinateOrdering(coordinates)));
    }
}
