/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.event;

import java.util.Map;

import com.salesforce.apollo.crypto.JohnHancock;

/**
 * @author hal.hildebrand
 *
 */
public interface AttachmentEvent extends KeyEvent {
    Map<Integer, JohnHancock> getEndorsements();

    Map<EventCoordinates, Map<Integer, JohnHancock>> getReceipts();

    @Override
    Map<Integer, JohnHancock> getAuthentication();
}
