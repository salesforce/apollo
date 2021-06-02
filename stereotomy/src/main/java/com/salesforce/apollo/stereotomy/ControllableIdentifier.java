/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy;

import java.util.List;

import com.salesforce.apollo.stereotomy.event.KeyEvent;
import com.salesforce.apollo.stereotomy.event.Seal;
 

/**
 * @author hal.hildebrand
 *
 */
public interface ControllableIdentifier extends KeyState {

    void rotate();

    void rotate(List<Seal> seals);

    void seal(List<Seal> seals);

    EventSignature sign(KeyEvent event);

  }