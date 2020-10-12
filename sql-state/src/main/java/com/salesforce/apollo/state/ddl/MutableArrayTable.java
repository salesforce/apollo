/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.ddl;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;

/**
 * @author hal.hildebrand
 *
 */
/** Table backed by a Java list. */
class MutableArrayTable extends AbstractModifiableTable implements Wrapper {
    final List<?>                              rows = new ArrayList<Object>();
    private final RelProtoDataType             protoRowType;
    private final InitializerExpressionFactory initializerExpressionFactory;

    /**
     * Creates a MutableArrayTable.
     *
     * @param name                         Name of table within its schema
     * @param protoStoredRowType           Prototype of row type of stored columns
     *                                     (all columns except virtual columns)
     * @param protoRowType                 Prototype of row type (all columns)
     * @param initializerExpressionFactory How columns are populated
     */
    MutableArrayTable(String name, RelProtoDataType protoStoredRowType, RelProtoDataType protoRowType,
            InitializerExpressionFactory initializerExpressionFactory) {
        super(name);
        Objects.requireNonNull(protoStoredRowType);
        this.protoRowType = Objects.requireNonNull(protoRowType);
        this.initializerExpressionFactory = Objects.requireNonNull(initializerExpressionFactory);
    }

    public Collection<?> getModifiableCollection() {
        return rows;
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
            @SuppressWarnings("unchecked")
            public Enumerator<T> enumerator() {
                // noinspection unchecked
                return (Enumerator<T>) Linq4j.enumerator(rows);
            }
        };
    }

    public Type getElementType() {
        return Object[].class;
    }

    public Expression getExpression(SchemaPlus schema, String tableName, @SuppressWarnings("rawtypes") Class clazz) {
        return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return protoRowType.apply(typeFactory);
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(initializerExpressionFactory)) {
            return aClass.cast(initializerExpressionFactory);
        }
        return super.unwrap(aClass);
    }
}
