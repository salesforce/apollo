/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.utils.proto.Digeste;

/**
 * @author hal.hildebrand
 *
 */
public interface ProtoKERLService {

    CompletableFuture<List<KeyState_>> append(KERL_ kerl);

    CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events);

    CompletableFuture<List<KeyState_>> append(List<KeyEvent_> events, List<AttachmentEvent> attachments);

    Optional<Attachment> getAttachment(EventCoords coordinates);

    Optional<KERL_> getKERL(Ident identifier);

    Optional<KeyEvent_> getKeyEvent(Digeste digest);

    Optional<KeyEvent_> getKeyEvent(EventCoords coordinates);

    Optional<KeyState_> getKeyState(EventCoords coordinates);

    Optional<KeyState_> getKeyState(Ident identifier);

    CompletableFuture<Void> publish(KERL_ kerl);

    CompletableFuture<Void> publishAttachments(List<AttachmentEvent> attachments);

    CompletableFuture<Void> publishEvents(List<KeyEvent_> events);

}
