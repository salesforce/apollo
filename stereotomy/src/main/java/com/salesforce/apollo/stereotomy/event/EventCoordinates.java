/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import com.salesforce.apollo.stereotomy.crypto.Digest;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface EventCoordinates {

    EventCoordinates NONE = new EventCoordinates() {

        @Override
        public Digest getDigest() {
            return Digest.NONE;
        }

        @Override
        public Identifier getIdentifier() {
            return Identifier.NONE;
        }

        @Override
        public long getSequenceNumber() {
            return -1;
        }
    };

    Digest getDigest();

    Identifier getIdentifier();

    long getSequenceNumber();

}
