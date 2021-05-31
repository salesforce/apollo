/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.sterotomy.event;

import java.util.List;

/**
 * @author hal.hildebrand
 *
 */
public interface InteractionEvent extends KeyEvent, SealingEvent {

    List<Seal> seals();

}
