/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.service;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.Any;
import com.salesfoce.apollo.stereotomy.event.proto.Bound;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface Resolver {

    void bind(Identifier prefix, Any value, JohnHancock signature, Duration timeout) throws TimeoutException;

    void bind(Identifier prefix, KeyState keystate, Duration timeout) throws TimeoutException;

    Optional<Bound> lookup(Identifier prefix, Duration timeout) throws TimeoutException;

    Optional<KeyState> resolve(Identifier prefix, Duration timeout) throws TimeoutException;

    Optional<KeyState> resolve(EventCoordinates coordinates, Duration timeout) throws TimeoutException;
}
