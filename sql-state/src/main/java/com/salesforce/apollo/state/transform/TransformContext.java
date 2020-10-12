/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.transform;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlNode;

/**
 * @author hal.hildebrand
 *
 */
public class TransformContext {

    @SuppressWarnings("unused")
    private SqlNode             currentStatement;
    @SuppressWarnings("unused")
    private final List<SqlNode> previousStatements = new ArrayList<>();
}
