/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium.vm;

import java.util.function.BiConsumer;

import com.google.protobuf.Any;

/**
 * @author hal.hildebrand
 *
 */
public interface Registry {

    void register(String discriminator, BiConsumer<String, Any> handler);

    void deregister(String discriminator);

}
