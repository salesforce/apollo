/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import java.util.concurrent.CompletableFuture;

import com.salesfoce.apollo.stereotomy.event.proto.KERL;
import com.salesfoce.apollo.stereotomy.event.proto.KeyEvent;

/**
 * @author hal.hildebrand
 *
 */
public interface ProtoKERLRecorder {

    CompletableFuture<Boolean> publish(KERL kerl);

    CompletableFuture<Boolean> append(KeyEvent keyEvent);
}
