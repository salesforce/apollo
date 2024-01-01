/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

/**
 * @author hal.hildebrand
 */
public interface DigestKERL extends KERL.AppendKERL {

    KeyEvent getKeyEvent(Digest digest);

}
