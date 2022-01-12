/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.event.InceptionEvent;
import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * Provides resolution functions, similar to DNS and PKI.
 * 
 * @author hal.hildebrand
 *
 */
public interface ResolverService {
    /**
     * Bindings may be made between non transferable identifiers and any of the
     * available Bound value types. Bindings are the signed Bound value by the key
     * of the identifier of the binding.
     */
    interface BinderService {
        void bind(Binding binding) throws TimeoutException;

        void unbind(Identifier identifier) throws TimeoutException;
    }

    public record Binding(Bound value, JohnHancock signature) {}

    public record Bound(InceptionEvent identifier, URI uri) {}

    /**
     * Answer the known extent of the key event receipt log for the supplied
     * identifier prefix
     */
    Optional<List<KeyEvent>> kerl(Identifier prefix) throws TimeoutException;

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
