/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.model.comms;

import com.salesfoce.apollo.demesne.proto.DelegationUpdate;
import com.salesfoce.apollo.cryptography.proto.Biff;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface DelegationService {
    DelegationUpdate gossip(Biff identifiers, Digest from);

    void update(DelegationUpdate update, Digest from);
}
