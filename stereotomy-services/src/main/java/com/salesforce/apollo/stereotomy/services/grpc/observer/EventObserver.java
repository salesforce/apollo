/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.observer;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.Validations;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface EventObserver {

    CompletableFuture<Void> publish(KERL_ kerl, List<Validations> validations, Digest from);

    CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments, Digest from);

    CompletableFuture<Void> publishEvents(List<KeyEvent_> events, List<Validations> validations, Digest from);
}
