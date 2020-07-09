/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;

/**
 * 
 * @author hal.hildebrand
 *
 */
public class ChainTable extends AbstractQueryableTable implements TranslatableTable, ScannableTable, ModifiableTable {

    private byte[] hash;

    public byte[] getHash() {
        return hash;
    }

    public void setHash(byte[] hash) {
        this.hash = hash;
    }

    protected ChainTable(Type elementType) {
        super(elementType);
        // TODO Auto-generated constructor stub
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<?> getModifiableCollection() {
        return null;
    }

    @Override
    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, CatalogReader catalogReader,
                                         RelNode child, Operation operation, List<String> updateColumnList,
                                         List<RexNode> sourceExpressionList, boolean flattened) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        // TODO Auto-generated method stub
        return null;
    }

}
