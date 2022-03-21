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
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;

/**
 * @author hal.hildebrand
 *
 */
public interface ProtoKERLService extends ProtoKERLProvider {

    CompletableFuture<List<KeyState_>> append(KERL_ kerl);

    CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events);

    CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events, List<AttachmentEvent> attachments);

}
