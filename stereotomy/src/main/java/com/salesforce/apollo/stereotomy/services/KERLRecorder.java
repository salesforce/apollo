/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.salesforce.apollo.stereotomy.KERL.EventWithAttachments;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

/**
 * @author hal.hildebrand
 *
 */
public interface KERLRecorder {
    void append(EventWithAttachments event);

    void publish(List<EventWithAttachments> kerl);

    CompletableFuture<KeyState> appendWithReturn(KeyEvent event);

    CompletableFuture<List<KeyState>> publishWithReturn(List<EventWithAttachments> kerl);
}
