/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.ddl;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * @author hal.hildebrand
 *
 */
/** Abstract base class for implementations of {@link ModifiableTable}. */
abstract class AbstractModifiableTable extends AbstractTable implements ModifiableTable {
    AbstractModifiableTable(String tableName) {
        super();
    }

    @Override
    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader,
                                         RelNode child, TableModify.Operation operation, List<String> updateColumnList,
                                         List<RexNode> sourceExpressionList, boolean flattened) {
        return LogicalTableModify.create(table, catalogReader, child, operation, updateColumnList, sourceExpressionList,
                                         flattened);
    }
}
