/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.util.Map;

import com.salesforce.apollo.stereotomy.crypto.Digest;
import com.salesforce.apollo.stereotomy.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface KeyEvent {

    Map<Integer, JohnHancock> getAuthentication();

    Format getFormat();

    byte[] getBytes();

    EventCoordinates getCoordinates();

    default Digest getDigest() {
        return this.getCoordinates().getDigest();
    }

    Map<Integer, JohnHancock> getEndorsements();

    default Identifier getIdentifier() {
        return this.getCoordinates().getIdentifier();
    }

    EventCoordinates getPrevious();

    Map<EventCoordinates, Map<Integer, JohnHancock>> getReceipts();

    default long getSequenceNumber() {
        return this.getCoordinates().getSequenceNumber();
    }

    Version getVersion();

}
