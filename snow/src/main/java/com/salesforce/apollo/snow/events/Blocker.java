/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.events;

import com.salesforce.apollo.snow.consensus.snowstorm.Common.acceptor;
import com.salesforce.apollo.snow.consensus.snowstorm.Common.rejector;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 *
 */
public class Blocker {

    public void fulfill(ID txID) {
        // TODO Auto-generated method stub
        
    }

    public void abandon(ID txID) {
        // TODO Auto-generated method stub
        
    }

    public void register(acceptor toAccept) {
        // TODO Auto-generated method stub
        
    }

    public void register(rejector toReject) {
        // TODO Auto-generated method stub
        
    }

}
