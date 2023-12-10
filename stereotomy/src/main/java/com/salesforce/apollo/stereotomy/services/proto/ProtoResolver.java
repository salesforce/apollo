/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import java.util.Optional;

import com.salesforce.apollo.stereotomy.event.proto.Binding;
import com.salesforce.apollo.stereotomy.event.proto.Ident;

/**
 * @author hal.hildebrand
 */
public interface ProtoResolver {
    /**
     * Answer the binding associated with the non transferable identifier
     */
    Optional<Binding> lookup(Ident prefix);
}
