/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.grpc.observer;

import com.salesforce.apollo.archipelago.Link;
import com.salesforce.apollo.stereotomy.services.proto.ProtoEventObserver;

/**
 * @author hal.hildebrand
 *
 */
public interface EventObserverService extends ProtoEventObserver, Link {

}
