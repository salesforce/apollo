/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.chain;

import com.salesforce.apollo.avro.HASH;

public interface Scope {
    <T> Chain<T> get(HASH key, Class<T> clazz);
}
