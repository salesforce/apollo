/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership.messaging;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesfoce.apollo.messaging.proto.MessageBff;
import com.salesfoce.apollo.messaging.proto.Messages;
import com.salesfoce.apollo.messaging.proto.Push;

/**
 * @author hal.hildebrand
 *
 */
public interface Messaging {

    ListenableFuture<Messages> gossip(MessageBff bff);

    void update(Push push);

}
