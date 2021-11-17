/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.gossip;

import com.salesfoce.apollo.choam.proto.Have;
import com.salesfoce.apollo.choam.proto.Update;
import com.salesforce.apollo.crypto.Digest;

/**
 * @author hal.hildebrand
 *
 */
public interface GossipService {

    Update gossip(Have request, Digest from);

}
