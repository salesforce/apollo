/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Ident: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services;

import java.util.Optional;
import java.util.concurrent.TimeoutException;

import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState;

/**
 * Provides resolution functions, similar to DNS and PKI.
 * 
 * @author hal.hildebrand
 *
 */
public interface ProtoResolverService {
    /**
     * Bindings may be made between non transferable identifiers and any of the
     * available Bound value types. Bindings are the signed Bound value by the key
     * of the identifier of the binding.
     */
    interface BinderService {
        void bind(Binding binding) throws TimeoutException;

        void unbind(Ident identifier) throws TimeoutException;
    }

    /**
     * Answer the KERL of the identifier prefix
     */
    Optional<KERL> kerl(Ident prefix);

    /**
     * Answer the binding associated with the non transferable identifier
     */
    Optional<Binding> lookup(Ident prefix);

    /**
     * Resolve the key state for the supplied event coordinates
     */
    Optional<KeyState> resolve(EventCoords coordinates);

    /**
     * Resolve the current key state of the identifier prefix
     */
    Optional<KeyState> resolve(Ident prefix);
}
