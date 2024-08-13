/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import com.salesforce.apollo.stereotomy.event.proto.*;
import org.joou.ULong;

/**
 * @author hal.hildebrand
 */
public interface ProtoKERLProvider {
    Attachment getAttachment(EventCoords coordinates);

    KERL_ getKERL(Ident identifier);

    KeyEvent_ getKeyEvent(EventCoords coordinates);

    KeyState_ getKeyState(EventCoords coordinates);

    KeyState_ getKeyState(Ident identifier, ULong sequenceNumber);

    KeyState_ getKeyState(Ident identifier);

    KeyState_ getKeyStateSeqNum(IdentAndSeq request);

    KeyStateWithAttachments_ getKeyStateWithAttachments(EventCoords coords);

    KeyStateWithEndorsementsAndValidations_ getKeyStateWithEndorsementsAndValidations(EventCoords coordinates);

    Validations getValidations(EventCoords coords);
}
