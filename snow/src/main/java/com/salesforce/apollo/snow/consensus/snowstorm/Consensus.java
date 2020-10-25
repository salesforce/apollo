/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowstorm;

import java.util.Collection;
import java.util.Set;

import com.salesforce.apollo.snow.consensus.snowball.Parameters;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public interface Consensus {

    void accept(ID txID);

    void add(Tx tx);

    Set<ID> conflicts(Tx tx);

    boolean finalized();

    boolean issued(Tx tx);

    Parameters parameters();

    Set<ID> preferences();

    boolean quiesce();
    
    void reject(ID... rejected); 

}
