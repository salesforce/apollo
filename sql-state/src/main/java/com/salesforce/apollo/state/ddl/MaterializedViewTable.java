/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.ddl;

import org.apache.calcite.materialize.MaterializationKey;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

/**
 * @author hal.hildebrand
 *
 */
class MaterializedViewTable extends MutableArrayTable {
    /**
     * The key with which this was stored in the materialization service, or null if
     * not (yet) materialized.
     */
    MaterializationKey key;

    MaterializedViewTable(String name, RelProtoDataType protoRowType) {
        super(name, protoRowType, protoRowType, NullInitializerExpressionFactory.INSTANCE);
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.MATERIALIZED_VIEW;
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        if (MaterializationKey.class.isAssignableFrom(aClass) && aClass.isInstance(key)) {
            return aClass.cast(key);
        }
        return super.unwrap(aClass);
    }
}
