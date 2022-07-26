/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEventWithAttachments.Builder;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesforce.apollo.stereotomy.event.InteractionEvent;
import com.salesforce.apollo.stereotomy.event.Seal;

/**
 * @author hal.hildebrand
 *
 */
public class InteractionEventImpl extends KeyEventImpl implements InteractionEvent {

    private final com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent event;

    public InteractionEventImpl(com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent event) {
        super(event.getSpecification().getHeader(), event.getCommon());
        this.event = event;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof InteractionEventImpl)) {
            return false;
        }
        InteractionEventImpl other = (InteractionEventImpl) obj;
        return Objects.equals(event, other.event);
    }

    @Override
    public List<Seal> getSeals() {
        List<Seal> seals = event.getSpecification()
                                .getSealsList()
                                .stream()
                                .map(s -> Seal.from(s))
                                .collect(Collectors.toList());
        return seals;
    }

    @Override
    public int hashCode() {
        return Objects.hash(event);
    }

    @Override
    public void setEventOf(Builder builder) {
        builder.setInteraction(event);
    }

    @Override
    public KeyEvent_ toKeyEvent_() {
        return KeyEvent_.newBuilder().setInteraction(event).build();
    }

    @Override
    protected ByteString toByteString() {
        return event.toByteString();
    }

}
