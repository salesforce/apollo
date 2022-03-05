/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import java.util.Optional;

import com.salesfoce.apollo.stereotomy.event.proto.EventCoords;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;
import com.salesfoce.apollo.stereotomy.event.proto.KERL_;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState_;

/**
 * @author hal.hildebrand
 *
 */
public interface ProtoKERLProvider {

    /**
     * Answer the KERL of the identifier prefix
     */
    Optional<KERL_> kerl(Ident prefix);

    /**
     * Resolve the key state for the supplied event coordinates
     */
    Optional<KeyState_> resolve(EventCoords coordinates);

    /**
     * Resolve the current key state of the identifier prefix
     */
    Optional<KeyState_> resolve(Ident prefix);
}
