/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowstorm;

import java.util.Collection;

import com.salesforce.apollo.snow.choices.Decidable;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public interface Tx extends Decidable {

    byte[] bytes();

    Collection<Tx> dependencies();

    Collection<ID> inputIDs();

    void reject();

}
