/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import com.salesforce.apollo.stereotomy.KeyEventLog;
import com.salesforce.apollo.stereotomy.KeyEventReceiptLog;
import com.salesforce.apollo.stereotomy.processing.KeyEventProcessor;
import com.salesforce.apollo.stereotomy.service.Controller;

/**
 * @author hal.hildebrand
 *
 */
public class Promulgator implements Controller {

    public Promulgator(KeyEventLog kel, KeyEventReceiptLog kerl, KeyEventProcessor processor) {
        this.kel = kel;
        this.kerl = kerl;
        this.processor = processor;
    }

    private final KeyEventLog        kel;
    private final KeyEventReceiptLog kerl;
    private final KeyEventProcessor  processor;
}
