/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.protocols;

import com.salesfoce.apollo.proto.MessageBff;
import com.salesfoce.apollo.proto.Messages;
import com.salesfoce.apollo.proto.Push;

/**
 * @author hal.hildebrand
 *
 */
public interface Messaging {

    Messages gossip(MessageBff bff);

    void update(Push push);

}
