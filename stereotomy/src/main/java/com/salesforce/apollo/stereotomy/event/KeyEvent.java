/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.util.Map;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface KeyEvent {

    default EventCoordinates getCoordinates() {
        return new EventCoordinates(getIdentifier(), getSequenceNumber(), getPriorEventDigest());
    }

    Digest getPriorEventDigest();

    Format getFormat();

    Identifier getIdentifier();

    EventCoordinates getPrevious();

    long getSequenceNumber();

    Version getVersion();

    Digest hash(DigestAlgorithm digest);

    Map<Integer, JohnHancock> getAuthentication();

    byte[] getBytes();
}
