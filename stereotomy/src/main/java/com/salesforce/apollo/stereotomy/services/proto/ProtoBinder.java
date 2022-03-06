/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.stereotomy.services.proto;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import com.salesfoce.apollo.stereotomy.event.proto.Binding;
import com.salesfoce.apollo.stereotomy.event.proto.Ident;

/**
 * @author hal.hildebrand
 *
 */
public interface ProtoBinder {
    CompletableFuture<Boolean> bind(Binding binding) throws TimeoutException;

    CompletableFuture<Boolean> unbind(Ident identifier) throws TimeoutException;
}
