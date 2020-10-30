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
public interface Batch extends KeyValueWriter {
    int valueSize();

    void write();

    void reset();

    void replay(KeyValueWriter w);

    Batch inner();
}
