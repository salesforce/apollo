/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.thoth.grpc.dht;

import com.google.protobuf.Empty;
import com.salesfoce.apollo.stereotomy.event.proto.*;
import com.salesfoce.apollo.stereotomy.services.grpc.proto.KeyStates;
import com.salesforce.apollo.archipelago.Link;

import java.util.List;

/**
 * @author hal.hildebrand
 */

public interface DhtService extends Link {

    KeyStates

    append(KERL_ kerl);

    KeyStates append(List<KeyEvent_> events);

    KeyStates append(List<KeyEvent_> events, List<AttachmentEvent> attachments);

    Empty appendAttachments(List<AttachmentEvent> attachments);

    Empty appendValidations(Validations attachments);

    Attachment getAttachment(EventCoords coordinates);

    KERL_ getKERL(Ident identifier);

    KeyEvent_ getKeyEvent(EventCoords coordinates);

    KeyState_ getKeyState(EventCoords coordinates);

    KeyState_ getKeyState(Ident identifier);

    KeyState_ getKeyState(IdentAndSeq identAndSeq);

    KeyStateWithAttachments_ getKeyStateWithAttachments(EventCoords coordinates);

    KeyStateWithEndorsementsAndValidations_ getKeyStateWithEndorsementsAndValidations(EventCoords coordinates);

    Validations getValidations(EventCoords coordinates);
}
