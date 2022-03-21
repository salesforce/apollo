/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.membership;

import java.io.InputStream;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.crypto.Signer;

/**
 * @author hal.hildebrand
 *
 */
public interface SigningMember extends Member, Signer {

    JohnHancock sign(InputStream message);
}
