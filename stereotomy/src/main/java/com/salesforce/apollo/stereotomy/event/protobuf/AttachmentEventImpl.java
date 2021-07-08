/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.util.Map;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.stereotomy.event.proto.Receipt;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.utils.Pair;

/**
 * @author hal.hildebrand
 *
 */
public class AttachmentEventImpl extends KeyEventImpl implements AttachmentEvent {

    private static Map<Integer, JohnHancock> signaturesOf(Receipt receipt) {
        return receipt.getSignaturesMap()
                      .entrySet()
                      .stream()
                      .collect(Collectors.toMap(e -> e.getKey(), e -> signature(e.getValue())));
    }

    private final com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent event;

    public AttachmentEventImpl(com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent event) {
        super(event.getHeader(), event.getCommon());
        this.event = event;
    }

    @Override
    public Map<Integer, JohnHancock> getEndorsements() {
        return event.getEndorsementsMap()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(e -> e.getKey(), e -> signature(e.getValue())));
    }

    @Override
    public Map<EventCoordinates, Map<Integer, JohnHancock>> getReceipts() {
        return event.getReceiptsList()
                    .stream()
                    .map(receipt -> new Pair<EventCoordinates, Map<Integer, JohnHancock>>(
                            EventCoordinates.from(receipt.getCoordinates()), signaturesOf(receipt)))
                    .collect(Collectors.toMap(e -> e.a, e -> e.b));
    }

    @Override
    protected ByteString toByteString() {
        return event.toByteString();
    }
}
