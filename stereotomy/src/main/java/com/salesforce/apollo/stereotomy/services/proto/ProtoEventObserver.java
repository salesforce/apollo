/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;

/**
 * @author hal.hildebrand
 *
 */
public interface ProtoEventObserver {

    CompletableFuture<List<AttachmentEvent>> publish(KERL_ kerl);

    CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments);

    CompletableFuture<List<AttachmentEvent>> publishEvents(List<KeyEvent_> events);

}
