/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services;

import java.util.List;
import java.util.Optional;

import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public class KERLResolverService implements ProtoResolverService {

    private final KERL kerl;

    public KERLResolverService(KERL kerl) {
        this.kerl = kerl;
    }

    @Override
    public Optional<com.salesfoce.apollo.stereotomy.event.proto.KERL> kerl(Ident prefix) {
        return kerl.kerl(Identifier.from(prefix)).map(kerl -> kerl(kerl));
    }

    @Override
    public Optional<Binding> lookup(Ident prefix) {
        return Optional.empty();
    }

    @Override
    public Optional<com.salesfoce.apollo.stereotomy.event.proto.KeyState> resolve(EventCoords coordinates) {
        return kerl.getKeyState(EventCoordinates.from(coordinates)).map(ks -> ks.toKeyState());
    }

    @Override
    public Optional<com.salesfoce.apollo.stereotomy.event.proto.KeyState> resolve(Ident prefix) {
        return kerl.getKeyState(Identifier.from(prefix)).map(ks -> ks.toKeyState());
    }

    private com.salesfoce.apollo.stereotomy.event.proto.KERL kerl(List<EventWithAttachments> kerl) {
        var builder = com.salesfoce.apollo.stereotomy.event.proto.KERL.newBuilder();
        kerl.forEach(ewa -> builder.addEvents(ewa.toKeyEvente()));
        return builder.build();
    }
}
