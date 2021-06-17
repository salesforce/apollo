/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc;

import com.salesforce.apollo.stereotomy.KEL;
import com.salesforce.apollo.stereotomy.KERL;
import com.salesforce.apollo.stereotomy.processing.KeyEventProcessor;
import com.salesforce.apollo.stereotomy.service.Controller;

/**
 * @author hal.hildebrand
 *
 */
public class Promulgator implements Controller {

    public Promulgator(KEL kel, KERL kerl, KeyEventProcessor processor) {
        this.kel = kel;
        this.kerl = kerl;
        this.processor = processor;
    }

    private final KEL        kel;
    private final KERL kerl;
    private final KeyEventProcessor  processor;
}
