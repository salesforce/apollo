/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event.protobuf;

import static com.salesforce.apollo.stereotomy.event.protobuf.ProtobufEventFactory.sealOf;

import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.salesforce.apollo.stereotomy.event.InteractionEvent;
import com.salesforce.apollo.stereotomy.event.Seal;

/**
 * @author hal.hildebrand
 *
 */
public class InteractionEventImpl extends KeyEventImpl implements InteractionEvent {

    private final com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent event;

    public InteractionEventImpl(com.salesfoce.apollo.stereotomy.event.proto.InteractionEvent event) {
        super(event.getHeader());
        this.event = event;
    }

    @Override
    public byte[] getBytes() {
        return event.toByteArray();
    }

    @Override
    public List<Seal> getSeals() {
        return event.getSealsList().stream().map(s -> sealOf(s)).collect(Collectors.toList());
    }

    @Override
    protected ByteString toByteString() {
        return event.toByteString();
    }

}
