/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.log;

import com.salesforce.apollo.consortium.Certification;

/**
 * @author hal.hildebrand
 *
 */
public class Block {
    private Header        header;
    private Body          body;
    private Certification cert;
}
