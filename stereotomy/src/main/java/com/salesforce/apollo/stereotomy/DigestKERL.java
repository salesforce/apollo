/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.concurrent.CompletableFuture;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.stereotomy.event.KeyEvent;

/**
 * @author hal.hildebrand
 *
 */
public interface DigestKERL extends KERL {

    CompletableFuture<KeyEvent> getKeyEvent(Digest digest);

}
