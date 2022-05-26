/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.stereotomy.event.proto.Attachment;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyStateWithAttachments_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;
import com.salesfoce.apollo.utils.proto.Digeste;

/**
 * @author hal.hildebrand
 *
 */
public interface ProtoKERLProvider {
    CompletableFuture<Attachment> getAttachment(EventCoords coordinates);

    CompletableFuture<KERL_> getKERL(Ident identifier);

    CompletableFuture<KeyEvent_> getKeyEvent(Digeste digest);

    CompletableFuture<KeyEvent_> getKeyEvent(EventCoords coordinates);

    CompletableFuture<KeyState_> getKeyState(EventCoords coordinates);

    CompletableFuture<KeyState_> getKeyState(Ident identifier);

    CompletableFuture<KeyStateWithAttachments_> getKeyStateWithAttachments(EventCoords coords);

}
