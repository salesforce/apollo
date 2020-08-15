/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.apollo.state.schema;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ResultSetEnumerable;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Queryable that gets its data from a table within a JDBC connection.
 *
 * <p>
 * The idea is not to read the whole table, however. The idea is to use this as
 * a building block for a query, by applying Queryable operators such as
 * {@link org.apache.calcite.linq4j.Queryable#where(org.apache.calcite.linq4j.function.Predicate2)}.
 * The resulting queryable can then be converted to a SQL query, which can be
 * executed efficiently on the JDBC server.
 * </p>
 */
class MaterializedView extends AbstractQueryableTable implements TranslatableTable, ScannableTable, ModifiableTable {
    /**
     * Enumerable that returns the contents of a {@link MaterializedView} by
     * connecting to the JDBC data source.
     */
    private class MaterializedViewQueryable<T> extends AbstractTableQueryable<T> {
        public MaterializedViewQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
            super(queryProvider, schema, MaterializedView.this, tableName);
        }

        public Enumerator<T> enumerator() {
            final JavaTypeFactory typeFactory = ((CalciteConnection) queryProvider).getTypeFactory();
            final SqlString sql = generateSql();
            // noinspection unchecked
            @SuppressWarnings("unchecked")
            final Enumerable<T> enumerable = (Enumerable<T>) ResultSetEnumerable.of(SubSchema.getDataSource(),
                                                                                    sql.getSql(),
                                                                                    JdbcUtils.ObjectArrayRowBuilder.factory(fieldClasses(typeFactory)));
            return enumerable.enumerator();
        }

        @Override
        public String toString() {
            return "MaterializedViewQueryable {table: " + tableName + "}";
        }
    }
    private final String           jdbcCatalogName;
    private final String           MaterializedViewName;
    private final Schema.TableType MaterializedViewType;
    private RelProtoDataType       protoRowType;
    private final SubSchema        SubSchema;

    private final String           SubSchemaName;

    public MaterializedView(SubSchema SubSchema, String jdbcCatalogName, String SubSchemaName, String tableName,
            Schema.TableType MaterializedViewType) {
        super(Object[].class);
        this.SubSchema = SubSchema;
        this.jdbcCatalogName = jdbcCatalogName;
        this.SubSchemaName = SubSchemaName;
        this.MaterializedViewName = tableName;
        this.MaterializedViewType = Preconditions.checkNotNull(MaterializedViewType);
    }

    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return new MaterializedViewQueryable<>(queryProvider, schema, tableName);
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return MaterializedViewType;
    }

    @Override
    public Collection<?> getModifiableCollection() {
        return null;
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoRowType == null) {
            try {
                protoRowType = SubSchema.getRelDataType(jdbcCatalogName, SubSchemaName, MaterializedViewName);
            } catch (SQLException e) {
                throw new RuntimeException("Exception while reading definition of table '" + MaterializedViewName + "'",
                        e);
            }
        }
        return protoRowType.apply(typeFactory);
    }

    public Enumerable<Object[]> scan(DataContext root) {
        final JavaTypeFactory typeFactory = root.getTypeFactory();
        final SqlString sql = generateSql();
        return ResultSetEnumerable.of(SubSchema.getDataSource(), sql.getSql(),
                                      JdbcUtils.ObjectArrayRowBuilder.factory(fieldClasses(typeFactory)));
    }

    @Override
    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, CatalogReader catalogReader,
                                         RelNode input, Operation operation, List<String> updateColumnList,
                                         List<RexNode> sourceExpressionList, boolean flattened) {
        SubSchema.convention.register(cluster.getPlanner());

        return new LogicalTableModify(cluster, cluster.traitSetOf(Convention.NONE), table, catalogReader, input,
                operation, updateColumnList, sourceExpressionList, flattened);
    }

    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new MaterializedViewScan(context.getCluster(), relOptTable, this, SubSchema.convention);
    }

    public String toString() {
        return "MaterializedView {" + MaterializedViewName + "}";
    }

    SqlString generateSql() {
        final SqlNodeList selectList = new SqlNodeList(Collections.singletonList(SqlIdentifier.star(SqlParserPos.ZERO)),
                SqlParserPos.ZERO);
        SqlSelect node = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList, tableName(), null, null, null,
                null, null, null, null, null);
        @SuppressWarnings("deprecation")
        final SqlPrettyWriter writer = new SqlPrettyWriter(SubSchema.dialect);
        node.unparse(writer, 0, 0);
        return writer.toSqlString();
    }

    SqlIdentifier tableName() {
        final List<String> strings = new ArrayList<>();
        if (SubSchema.catalog != null) {
            strings.add(SubSchema.catalog);
        }
        if (SubSchema.schema != null) {
            strings.add(SubSchema.schema);
        }
        strings.add(MaterializedViewName);
        return new SqlIdentifier(strings, SqlParserPos.ZERO);
    }

    private List<Pair<ColumnMetaData.Rep, Integer>> fieldClasses(final JavaTypeFactory typeFactory) {
        final RelDataType rowType = protoRowType.apply(typeFactory);
        return Lists.transform(rowType.getFieldList(),
                               new Function<RelDataTypeField, Pair<ColumnMetaData.Rep, Integer>>() {
                                   public Pair<ColumnMetaData.Rep, Integer> apply(RelDataTypeField field) {
                                       final RelDataType type = field.getType();
                                       final Class<?> clazz = (Class<?>) typeFactory.getJavaClass(type);
                                       final ColumnMetaData.Rep rep = Util.first(ColumnMetaData.Rep.of(clazz),
                                                                                 ColumnMetaData.Rep.OBJECT);
                                       return Pair.of(rep, type.getSqlTypeName().getJdbcOrdinal());
                                   }
                               });
    }
}

// End MaterializedView.java
