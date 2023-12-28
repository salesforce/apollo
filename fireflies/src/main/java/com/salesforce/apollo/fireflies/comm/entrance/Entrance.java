/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies.comm.entrance;

import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.fireflies.View.Node;
import com.salesforce.apollo.fireflies.proto.*;
import com.salesforce.apollo.membership.Member;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.event.proto.IdentAndSeq;
import com.salesforce.apollo.stereotomy.event.proto.KeyState_;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import org.joou.ULong;

import java.io.IOException;
import java.time.Duration;

/**
 * @author hal.hildebrand
 */
public interface Entrance extends Link {

    static Entrance getLocalLoopback(Node node, EntranceService service) {
        return new Entrance() {

            @Override
            public void close() throws IOException {
            }

            @Override
            public KeyState_ getKeyState(IdentAndSeq idSeq) {

                return service.getKeyState(Identifier.from(idSeq.getIdentifier()),
                                           ULong.valueOf(idSeq.getSequenceNumber()), getMember().getId()).toKeyState_();
            }

            @Override
            public KeyState_ getKeyState(EventCoords coords) {
                return service.getKeyState(EventCoordinates.from(coords), getMember().getId()).toKeyState_();
            }

            @Override
            public Member getMember() {
                return node;
            }

            @Override
            public Gateway join(Join join, Duration timeout) {
                return null;
            }

            @Override
            public Redirect seed(Registration registration) {
                return null;
            }

            @Override
            public Validation validate(EventCoords coords) {
                return service.validateCoords(coords, getMember().getId());
            }
        };
    }

    KeyState_ getKeyState(IdentAndSeq idSeq);

    KeyState_ getKeyState(EventCoords coords);

    Gateway join(Join join, Duration timeout);

    Redirect seed(Registration registration);

    Validation validate(EventCoords coords);
}
