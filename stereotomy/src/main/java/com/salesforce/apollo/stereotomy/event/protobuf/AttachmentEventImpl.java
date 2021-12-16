/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.crypto.QualifiedBase64.signature;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.utils.Pair;

/**
 * @author hal.hildebrand
 *
 */
public class AttachmentEventImpl extends KeyEventImpl implements AttachmentEvent {

    private final com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent event;

    public AttachmentEventImpl(com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent event) {
        super(event.getHeader(), event.getCommon());
        this.event = event;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof AttachmentEventImpl)) {
            return false;
        }
        AttachmentEventImpl other = (AttachmentEventImpl) obj;
        return Objects.equals(event, other.event);
    }

    @Override
    public List<JohnHancock> getEndorsements() {
        return event.getEndorsementsList().stream().map(e -> signature(e)).toList();
    }

    @Override
    public Map<EventCoordinates, JohnHancock> getReceipts() {
        Stream<Pair<EventCoordinates, JohnHancock>> map = event.getReceiptsList().stream()
                                                               .map(receipt -> new Pair<>(EventCoordinates.from(receipt.getCoordinates()),
                                                                                          signature(receipt.getSignatures())));
        return map.collect(Collectors.toMap(e -> e.a, e -> e.b));
    }

    @Override
    public int hashCode() {
        return Objects.hash(event);
    }

    @Override
    protected ByteString toByteString() {
        return event.toByteString();
    }
}
