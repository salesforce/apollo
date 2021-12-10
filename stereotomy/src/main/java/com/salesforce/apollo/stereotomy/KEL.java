/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.Optional;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.event.DelegatingEventCoordinates;
import com.salesforce.apollo.stereotomy.event.EventCoordinates;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.SealingEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface KEL {

    Optional<KeyState> getKeyState(Identifier identifier);

    Optional<KeyState> getKeyState(EventCoordinates coordinates);

    Optional<KeyEvent> getKeyEvent(EventCoordinates coordinates);

    Optional<KeyEvent> getKeyEvent(Digest digest);

    Optional<SealingEvent> getKeyEvent(DelegatingEventCoordinates coordinates);

    DigestAlgorithm getDigestAlgorithm();

    void append(KeyEvent event, KeyState newState);

}
