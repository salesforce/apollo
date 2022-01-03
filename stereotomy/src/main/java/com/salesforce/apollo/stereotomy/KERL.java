/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import com.salesforce.apollo.stereotomy.event.AttachmentEvent;

/**
 * The Key Event Receipt Log
 * 
 * @author hal.hildebrand
 *
 */
public interface KERL extends KEL {

    void append(AttachmentEvent event);

}
