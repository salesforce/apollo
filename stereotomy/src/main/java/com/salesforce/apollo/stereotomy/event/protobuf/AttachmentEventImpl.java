/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
import com.salesforce.apollo.stereotomy.event.Version;

/**
 * @author hal.hildebrand
 *
 */
public class AttachmentEventImpl implements AttachmentEvent {

    private final com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent event;

    public AttachmentEventImpl(com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent event) {
        this.event = event;
    }

    @Override
    public Attachment attachments() {
        return new Attachment() {

            @Override
            public Map<Integer, JohnHancock> endorsements() {
                return event.getAttachment()
                            .getEndorsementsMap()
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(e -> e.getKey(), e -> JohnHancock.of(e.getValue())));
            }

            @Override
            public List<Seal> seals() {
                return event.getAttachment().getSealsList().stream().map(s -> Seal.from(s)).toList();
            }
        };
    }

    @Override
    public EventCoordinates coordinates() {
        return EventCoordinates.from(event.getCoordinates());
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
    public int hashCode() {
        return Objects.hash(event);
    }

    @Override
    public Version version() {
        return new Version() {

            @Override
            public int getMajor() {
                return event.getVersion().getMajor();
            }

            @Override
            public int getMinor() {
                return event.getVersion().getMinor();
            }
        };
    }

    protected ByteString toByteString() {
        return event.toByteString();
    }
}
