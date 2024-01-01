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
import com.salesforce.apollo.stereotomy.event.proto.EventCoords;
import com.salesforce.apollo.stereotomy.event.proto.IdentAndSeq;
import com.salesforce.apollo.stereotomy.event.proto.KeyState_;

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
            public Member getMember() {
                return node;
            }

            @Override
            public Gateway join(Join join, Duration timeout) {
                return null;
            }

            @Override
            public KeyState_ keyState(IdentAndSeq idAndSeq) {
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

    Gateway join(Join join, Duration timeout);

    KeyState_ keyState(IdentAndSeq idAndSeq);

    Redirect seed(Registration registration);

    Validation validate(EventCoords coords);
}
