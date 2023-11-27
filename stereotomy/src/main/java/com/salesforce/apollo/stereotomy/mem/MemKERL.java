/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.mem;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.cryptography.DigestAlgorithm;
import com.salesforce.apollo.cryptography.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent.Attachment;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.processing.KeyEventProcessor;
import org.joou.ULong;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.salesforce.apollo.cryptography.QualifiedBase64.qb64;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

/**
 * @author hal.hildebrand
 */
public class MemKERL implements KERL {

    private final DigestAlgorithm                                           digestAlgorithm;
    // Order by <stateOrdering>
    private final Map<String, KeyEvent>                                     events                   = new ConcurrentHashMap<>();
    private final Map<Digest, String>                                       eventsByHash             = new ConcurrentHashMap<>();
    // Order by <stateOrdering>
    private final Map<String, KeyState>                                     keyState                 = new ConcurrentHashMap<>();
    // Order by <identifier>
    private final Map<String, String>                                       keyStateByIdentifier     = new ConcurrentHashMap<>();
    private final Map<String, Digest>                                       locationToHash           = new ConcurrentHashMap<>();
    private final Map<String, String>                                       sequenceNumberToLocation = new ConcurrentHashMap<>();
    private final KeyEventProcessor                                         processor                = new KeyEventProcessor(
    this);
    // Order by <receiptOrdering>
    private final Map<String, Attachment>                                   receipts                 = new ConcurrentHashMap<>();
    // Order by <coordinateOrdering>
    private final Map<EventCoordinates, Map<EventCoordinates, JohnHancock>> validations              = new ConcurrentHashMap<>();

    public MemKERL(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

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

    public static String locationOrdering(Identifier identifier, ULong sequenceNumber) {
        return qb64(identifier) + ':' + sequenceNumber;
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

    @Override
    public KeyState append(KeyEvent event) {
        final var newState = processor.process(event);
        append(event, newState);
        return newState;
    }

    @Override
    public Void append(List<AttachmentEvent> events) {
        events.forEach(event -> appendAttachments(event.coordinates(), event.attachments()));
        return null;
    }

    @Override
    public List<KeyState> append(List<KeyEvent> events, List<AttachmentEvent> attachments) {
        var states = events.stream().map(event -> {
            return append(event);
        }).toList();
        append(attachments);
        return states;
    }

    @Override
    public Void appendValidations(EventCoordinates coordinates, Map<EventCoordinates, JohnHancock> v) {
        validations.put(coordinates, v);
        return null;
    }

    @Override
    public Attachment getAttachment(EventCoordinates coordinates) {
        return receipts.get(coordinateOrdering(coordinates));
    }

    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    @Override
    public KeyEvent getKeyEvent(EventCoordinates coordinates) {
        return events.get(coordinateOrdering(coordinates));
    }

    @Override
    public KeyState getKeyState(EventCoordinates coordinates) {
        return keyState.get(coordinateOrdering(coordinates));
    }

    @Override
    public KeyState getKeyState(Identifier identifier) {

        String stateHash = keyStateByIdentifier.get(qb64(identifier));

        return stateHash == null ? null : keyState.get(stateHash);
    }

    @Override
    public Map<EventCoordinates, JohnHancock> getValidations(EventCoordinates coordinates) {
        return validations.computeIfAbsent(coordinates, k -> Collections.emptyMap());
    }

    @Override
    public KeyState getKeyState(Identifier identifier, ULong sequenceNumber) {
        var location = sequenceNumberToLocation.get(locationOrdering(identifier, sequenceNumber));
        return location == null ? null : keyState.get(location);
    }

    private void append(KeyEvent event, KeyState newState) {
        String coordinates = coordinateOrdering(event.getCoordinates());
        events.put(coordinates, event);
        eventsByHash.put(newState.getDigest(), coordinates);
        locationToHash.put(coordinates, newState.getDigest());
        sequenceNumberToLocation.put(locationOrdering(event.getIdentifier(), event.getSequenceNumber()), coordinates);
        keyState.put(coordinates, newState);
        keyStateByIdentifier.put(qb64(event.getIdentifier()), coordinates);
    }

    private void appendAttachments(EventCoordinates coordinates, Attachment attachment) {
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

        return new Attachment() {

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
}
