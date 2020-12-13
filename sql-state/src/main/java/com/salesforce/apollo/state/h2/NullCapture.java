/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.h2;

import org.h2.engine.Session.CDC;
import org.h2.result.Row;
import org.h2.table.Table;

/**
 * @author hal.hildebrand
 *
 */
public class NullCapture implements Cdc {
    public static final NullCapture INSTANCE = new NullCapture();

    private NullCapture() {
    }

    @Override
    public void cdc(Table table, Row prev, CDC operation, Row row) {
    }

}
