/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.OptionalLong;

import com.salesforce.apollo.crypto.DigestAlgorithm;
import com.salesforce.apollo.stereotomy.event.AttachmentEvent;
import com.salesforce.apollo.stereotomy.identifier.Identifier;

/**
 * @author hal.hildebrand
 *
 */
public interface KeyEventReceiptLog {
    DigestAlgorithm getDigestAlgorithm();

    OptionalLong findLatestReceipt(Identifier forIdentifier, Identifier byIdentifier);

    void append(AttachmentEvent event, KeyState newState);

}
