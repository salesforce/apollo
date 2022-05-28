/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc;

import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.AttachmentEvent;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithAttachments_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.thoth.proto.KeyStateWithEndorsementsAndValidations;
import com.salesfoce.apollo.thoth.proto.Validations;
import com.salesforce.apollo.comm.Link;

/**
 * @author hal.hildebrand
 *
 */

public interface DhtService extends Link {

    ListenableFuture<Empty> append(KERL_ kerl);

    ListenableFuture<Empty> append(List<KeyEvent_> events);

    ListenableFuture<Empty> append(List<KeyEvent_> events, List<AttachmentEvent> attachments);

    ListenableFuture<Empty> appendAttachments(List<AttachmentEvent> attachments);

    ListenableFuture<Empty> appendValidations(List<Validations> attachments);

    ListenableFuture<Attachment> getAttachment(EventCoords coordinates);

    ListenableFuture<KERL_> getKERL(Ident identifier);

    ListenableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates);

    ListenableFuture<KeyState_> getKeyState(EventCoords coordinates);

    ListenableFuture<KeyState_> getKeyState(Ident identifier);

    ListenableFuture<KeyStateWithAttachments_> getKeyStateWithAttachments(EventCoords coordinates);

    ListenableFuture<Validations> getValidations(EventCoords coordinates);

    ListenableFuture<KeyStateWithEndorsementsAndValidations> getKeyStateWithEndorsementsAndValidations(EventCoords coordinates);
}
