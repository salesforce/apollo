/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.comms;

import com.google.common.util.concurrent.ListenableFuture;
import com.salesforce.apollo.demesne.proto.DelegationUpdate;
import com.salesforce.apollo.cryptography.proto.Biff;
import com.salesforce.apollo.archipelago.Link;

/**
 * @author hal.hildebrand
 */
public interface Delegation extends Link {

    DelegationUpdate gossip(Biff identifers);

    void update(DelegationUpdate update);

}
