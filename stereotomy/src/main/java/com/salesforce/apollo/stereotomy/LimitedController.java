/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.io.InputStream;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.identifier.Identifier;
import com.salesforce.apollo.stereotomy.identifier.SelfAddressingIdentifier;

/**
 * @author hal.hildebrand
 *
 */
public interface LimitedController {

    JohnHancock sign(SelfAddressingIdentifier identifier, InputStream message);

    boolean verify(Identifier identifier, JohnHancock signature, InputStream message);
}
