/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * Provides resolution functions, similar to DNS and PKI.
 * <p>
 * Bindings may be made between non transferable identifiers and any of the
 * available Bound types. Bindings are the signed Bound value by the key of the
 * identifier of the binding.
 * 
 * @author hal.hildebrand
 *
 */
public interface Resolver {

    void bind(Binding binding, Duration timeout) throws TimeoutException;

    Identifier identity();

    Optional<List<KeyEvent>> kel(Identifier prefix) throws TimeoutException;

    Optional<List<KeyEvent>> kerl(Identifier prefix) throws TimeoutException;

    Optional<Binding> lookup(Identifier prefix) throws TimeoutException;

    Optional<KeyState> resolve(EventCoordinates coordinates) throws TimeoutException;

    Optional<KeyState> resolve(Identifier prefix) throws TimeoutException;

    Optional<KeyState> resolve(KeyCoordinates coordinates) throws TimeoutException;
}
