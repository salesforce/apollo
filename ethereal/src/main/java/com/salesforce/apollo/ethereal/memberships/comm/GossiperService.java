/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.ethereal.memberships.comm;

import com.salesforce.apollo.ethereal.proto.ContextUpdate;
import com.salesforce.apollo.ethereal.proto.Gossip;
import com.salesforce.apollo.ethereal.proto.Update;
import com.salesforce.apollo.cryptography.Digest;

/**
 * @author hal.hildebrand
 */
public interface GossiperService {

    Update gossip(Gossip request, Digest from);

    void update(ContextUpdate request, Digest from);

}
