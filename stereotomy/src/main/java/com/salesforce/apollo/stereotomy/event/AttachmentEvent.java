/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.util.List;
import java.util.Map;

import com.salesforce.apollo.crypto.JohnHancock;
import com.salesforce.apollo.stereotomy.EventCoordinates;

/**
 * @author hal.hildebrand
 *
 */
public interface AttachmentEvent {
    interface Attachment {
        Map<Integer, JohnHancock> endorsements();

        List<Seal> seals();
    }

    Version version();

    EventCoordinates coordinates();

    Attachment attachments();
}
