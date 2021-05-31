/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.text.Format;
import java.util.Map;

import com.salesforce.apollo.stereotomy.controller.Coordinates;
import com.salesforce.apollo.stereotomy.crypto.Digest;
import com.salesforce.apollo.stereotomy.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface KeyEvent {

    Map<Integer, JohnHancock> authentication();

    byte[] bytes();

    Coordinates coordinates();

    default Digest digest() {
        return this.coordinates().digest();
    }

    Map<Integer, JohnHancock> endorsements();

    Format format();

    default Identifier identifier() {
        return this.coordinates().identifier();
    }

    Coordinates previous();

    Map<Coordinates, Map<Integer, JohnHancock>> receipts();

    default long sequenceNumber() {
        return this.coordinates().sequenceNumber();
    }

    Version version();

}
