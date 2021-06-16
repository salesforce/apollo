/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.service;

import java.util.Optional;

import com.google.protobuf.Any;
import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.KeyState;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface Resolver {

    Optional<Any> lookup(Identifier prefix);

    Optional<KeyState> resolve(Identifier prefix);

    void bind(Identifier prefix, Any value, JohnHancock signature);
}
