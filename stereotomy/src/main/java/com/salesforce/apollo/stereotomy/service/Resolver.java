/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.service;

import com.google.protobuf.Any;
import com.salesfoce.apollo.stereotomy.event.proto.KeyState;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface Resolver {

    Any lookup(Identifier prefix);
    
    KeyState resolve(Identifier prefix);
    
    void bind(Identifier prefix, Any value);
}
