/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.sql.SQLException;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import com.salesforce.apollo.state.ddl.ChainSchema;

/**
 * @author hal.hildebrand
 *
 */
public class State {
    @SuppressWarnings("unused")
    private final RelBuilder  builder;
    private final ChainSchema root;
    @SuppressWarnings("unused")
    private final SchemaPlus  rootSchema = CalciteSchema.createRootSchema(true).plus();

    public State(CdcEngine engine, String modelUri) throws SQLException {
        root = new ChainSchema(engine);
        builder = RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(root).build());
    }
}
