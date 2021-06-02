/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.store;

import static com.salesforce.apollo.crypto.QualifiedBase64.qb64;
import static com.salesforce.apollo.stereotomy.identifier.QualifiedBase64Identifier.qb64;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.h2.mvstore.MVMap;

import com.salesfoce.apollo.stereotomy.event.proto.KeyState;
import com.salesfoce.apollo.stereotomy.event.proto.Signatures;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.protobuf.KeyEventImpl;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class StateStore {

    private static String receiptDigestSuffix(EventCoordinates event, EventCoordinates signer) {
        return qb64(event.getDigest()) + '.' + qb64(signer.getDigest());
    }

    /**
     * Ording by
     * 
     * <pre>
     * <event.identifier, signer.identifier, event.sequenceNumber, signer.sequenceNumber, event.digest, signer.digest>
     * </pre>
     */
    private static String receiptOrdering(EventCoordinates event, EventCoordinates signer) {
        return receiptPrefix(event, signer) + receiptSequence(event, signer) + receiptDigestSuffix(event, signer);
    }

    private static String receiptPrefix(EventCoordinates event, EventCoordinates signer) {
        return receiptPrefix(event.getIdentifier(), signer.getIdentifier());
    }

    private static String receiptPrefix(Identifier forIdentifier, Identifier forIdentifier2) {
        return qb64(forIdentifier) + '.' + qb64(forIdentifier2) + '.';
    }

    private static String receiptSequence(EventCoordinates event, EventCoordinates signer) {
        return Long.toString(event.getSequenceNumber()) + '.' + signer.getSequenceNumber() + '.';
    }

    private static Signatures signatures(Map<Integer, JohnHancock> signatures) {
        return Signatures.newBuilder()
                         .putAllSignatures(signatures.entrySet()
                                                     .stream()
                                                     .collect(Collectors.toMap(e -> e.getKey(),
                                                                               e -> qb64(e.getValue()))))
                         .build();
    }

    /**
     * Ording by
     * 
     * <pre>
     * <coords.identifier, coords.sequenceNumber, coords.digest>
     * </pre>
     */
    private static String stateOrdering(EventCoordinates coords) {
        return qb64(coords.getIdentifier()) + '.' + coords.getSequenceNumber() + '.' + qb64(coords.getDigest());
    }

    // Order by <stateOrdering>
    private MVMap<String, Signatures> authentications;
    // Order by <stateOrdering>
    private MVMap<String, Signatures> endorsements;
    // Order by <stateOrdering>
    private MVMap<String, KeyEventImpl> events;
    // Order by <stateOrdering>
    private MVMap<String, KeyState> keyState;
    // Order by <identifier>
    private MVMap<String, String> keyStateByIdentifier;
    // Order by <receiptOrdering>
    private MVMap<String, Signatures> receipts;

    public void append(AttachmentEvent event) {
        this.appendAttachments(event.getCoordinates(), event.getAuthentication(), event.getEndorsements(),
                               event.getReceipts());
    }

    public void append(KeyEventImpl event, KeyState newState) {
        String stateHash = stateOrdering(event.getCoordinates());
        events.put(stateHash, event);
        keyState.put(stateHash, newState);
        keyStateByIdentifier.put(qb64(event.getIdentifier()), stateHash);
        appendAttachments(event.getCoordinates(), event.getAuthentication(), event.getEndorsements(),
                          event.getReceipts());
    }

    public OptionalLong findLatestReceipt(Identifier forIdentifier, Identifier byIdentifier) {
        String prefix = receiptPrefix(forIdentifier, byIdentifier);
        String ceiling = receipts.ceilingKey(prefix);
        return null;
    }

    public Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates) {
        return Optional.of(events.get(stateOrdering(coordinates)));
    }

    public Optional<KeyState> getKeyState(Identifier identifier) {
        String stateHash = keyStateByIdentifier.get(qb64(identifier));

        return stateHash == null ? Optional.empty() : Optional.of(keyState.get(stateHash));
    }

    private void appendAttachments(EventCoordinates coordinates, Map<Integer, JohnHancock> signatures,
                                   Map<Integer, JohnHancock> receipts,
                                   Map<EventCoordinates, Map<Integer, JohnHancock>> otherReceipts) {
        String stateHash = stateOrdering(coordinates);
        authentications.put(stateHash, signatures(signatures));
        endorsements.put(stateHash, signatures(receipts));
        for (var otherReceipt : otherReceipts.entrySet()) {
            var key = receiptOrdering(coordinates, otherReceipt.getKey());
            this.receipts.put(key, signatures(otherReceipt.getValue()));
        }
    }
}
