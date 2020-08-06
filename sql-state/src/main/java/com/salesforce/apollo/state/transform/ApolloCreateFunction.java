/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.transform;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlCreateFunction;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * @author hal.hildebrand
 *
 */
public class ApolloCreateFunction extends SqlCreateFunction {

    /**
     * @param pos
     * @param replace
     * @param ifNotExists
     * @param name
     * @param className
     * @param usingList
     */
    public ApolloCreateFunction(SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name,
            SqlNode className, SqlNodeList usingList) {
        super(pos, replace, ifNotExists, name, className, usingList);
    }

}
