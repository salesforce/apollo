/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.KEL;
import com.salesfoce.apollo.stereotomy.event.proto.KERL;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * Provides resolution functions, similar to DNS and PKI.
 * 
 * @author hal.hildebrand
 *
 */
public interface Resolver {

    /**
     * Bindings may be made between non transferable identifiers and any of the
     * available Bound value types. Bindings are the signed Bound value by the key
     * of the identifier of the binding.
     */
    interface Binder {
        void bind(Binding binding) throws TimeoutException;

        void unbind(Identifier identifier) throws TimeoutException;
    }

    /**
     * Answer the known extent of the key event log for the supplied identifier
     * prefix
     */
    Optional<KEL> kel(Identifier prefix) throws TimeoutException;

    /**
     * Answer the known extent of the key event receipt log for the supplied
     * identifier prefix
     */
    Optional<KERL> kerl(Identifier prefix) throws TimeoutException;

    /**
     * Answer the binding associated with the non transferable identifier
     */
    Optional<Binding> lookup(Identifier prefix) throws TimeoutException;

    /**
     * Resolve the key state for the supplied event coordinates
     */
    Optional<KeyState> resolve(EventCoordinates coordinates) throws TimeoutException;

    /**
     * Resolve the current key state of the identifier prefix
     */
    Optional<KeyState> resolve(Identifier prefix) throws TimeoutException;
}
