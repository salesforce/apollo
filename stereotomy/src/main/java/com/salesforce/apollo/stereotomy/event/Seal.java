/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface Seal {
    interface CoordinatesSeal extends Seal {
        static CoordinatesSeal construct(EventCoordinates coordinates) {
            return new CoordinatesSeal() {

                @Override
                public EventCoordinates getEvent() {
                    return coordinates;
                }
            };
        }

        EventCoordinates getEvent();
    }

    interface DigestSeal extends Seal {
        static DigestSeal construct(Digest digest) {
            return new DigestSeal() {

                @Override
                public Digest getDigest() {
                    return digest;
                }
            };
        }

        Digest getDigest();
    }
}
