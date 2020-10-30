/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.database;

/**
 * @author hal.hildebrand
 *
 */
public interface KeyValueWriter {
    void put(byte[] key, byte[] value);
    void delete(byte[] key);
}
