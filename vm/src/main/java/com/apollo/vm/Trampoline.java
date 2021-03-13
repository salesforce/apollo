/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.apollo.vm;

import com.google.protobuf.Any;

/**
 * @author hal.hildebrand
 *
 */
public interface Trampoline {
    class Event {
        public final Any    body;
        public final String discriminator;

        public Event(String discriminator, Any body) {
            this.discriminator = discriminator;
            this.body = body;
        }
    }

    void publish(Event event);

    void publish(String discriminator, Any body);

}
