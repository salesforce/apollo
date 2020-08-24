/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

/**
 * @author hal.hildebrand
 *
 */
public class ChainSchema implements SchemaPlus {

    public static class Factory implements SchemaFactory {
        public static Factory INSTANCE = new Factory();

        private Factory() {
        }

        @Override
        public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
            return SubSchema.create(parentSchema, name, operand);
        }
    }

    @SuppressWarnings("unused")
    private final String                        catalog;
    @SuppressWarnings("unused")
    private final JdbcConvention                convention;
    @SuppressWarnings("unused")
    private final DataSource                    dataSource;
    @SuppressWarnings("unused")
    private final SqlDialect                    dialect;
    @SuppressWarnings("unchecked")
    private final Multimap<String, Function>    functions    = (Multimap<String, Function>) MultimapBuilder.linkedHashKeys();
    private final String                        schema;
    @SuppressWarnings("unused")
    private final boolean                       snapshot;
    private final Map<String, SchemaPlus>       subSchemaMap = new HashMap<>();
    private final Map<String, Table>            tableMap     = new HashMap<>();
    private final Map<String, RelProtoDataType> typeMap      = new HashMap<>();

    private ChainSchema(DataSource dataSource, SqlDialect dialect, JdbcConvention convention, String catalog,
            String schema) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.dialect = Objects.requireNonNull(dialect);
        this.convention = convention;
        this.catalog = catalog;
        this.schema = schema;
        this.snapshot = tableMap != null;
    }

    @Override
    public void add(String name, Function function) {
        // TODO Auto-generated method stub

    }

    @Override
    public void add(String name, Lattice lattice) {
        // TODO Auto-generated method stub

    }

    @Override
    public void add(String name, RelProtoDataType type) {
        // TODO Auto-generated method stub

    }

    @Override
    public SchemaPlus add(String name, Schema schema) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void add(String name, Table table) {
        // TODO Auto-generated method stub

    }

    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return Schemas.subSchemaExpression(parentSchema, name, getClass());
    }

    public Set<String> getFunctionNames() {
        return getFunctionMultimap().keySet();
    }

    public Collection<Function> getFunctions(String name) {
        return getFunctionMultimap().get(name); // never null
    }

    @Override
    public String getName() {
        return schema;
    }

    @Override
    public SchemaPlus getParentSchema() {
        // TODO Auto-generated method stub
        return null;
    }

    public SchemaPlus getSubSchema(String name) {
        return getSubSchemaMap().get(name);
    }

    public Set<String> getSubSchemaNames() {
        return getSubSchemaMap().keySet();
    }

    public Table getTable(String name) {
        return getTableMap().get(name);
    }

    public Set<String> getTableNames() {
        return getTableMap().keySet();
    }

    public RelProtoDataType getType(String name) {
        return getTypeMap().get(name);
    }

    public Set<String> getTypeNames() {
        return getTypeMap().keySet();
    }

    @Override
    public boolean isCacheEnabled() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isMutable() {
        return true;
    }

    @Override
    public void setCacheEnabled(boolean cache) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setPath(ImmutableList<ImmutableList<String>> path) {
        // TODO Auto-generated method stub

    }

    public Schema snapshot(SchemaVersion version) {
        return this;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Returns a multi-map of functions in this schema by name. It is a multi-map
     * because functions are overloaded; there may be more than one function in a
     * schema with a given name (as long as they have different parameter lists).
     *
     * <p>
     * The implementations of {@link #getFunctionNames()} and
     * {@link Schema#getFunctions(String)} depend on this map. The default
     * implementation of this method returns the empty multi-map. Override this
     * method to change their behavior.
     * </p>
     *
     * @return Multi-map of functions in this schema by name
     */
    protected Multimap<String, Function> getFunctionMultimap() {
        return functions;
    }

    /**
     * Returns a map of sub-schemas in this schema by name.
     *
     * <p>
     * The implementations of {@link #getSubSchemaNames()} and
     * {@link #getSubSchema(String)} depend on this map. The default implementation
     * of this method returns the empty map. Override this method to change their
     * behavior.
     * </p>
     *
     * @return Map of sub-schemas in this schema by name
     */
    protected Map<String, SchemaPlus> getSubSchemaMap() {
        return subSchemaMap;
    }

    /**
     * Returns a map of tables in this schema by name.
     *
     * <p>
     * The implementations of {@link #getTableNames()} and {@link #getTable(String)}
     * depend on this map. The default implementation of this method returns the
     * empty map. Override this method to change their behavior.
     * </p>
     *
     * @return Map of tables in this schema by name
     */
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }

    /**
     * Returns a map of types in this schema by name.
     *
     * <p>
     * The implementations of {@link #getTypeNames()} and {@link #getType(String)}
     * depend on this map. The default implementation of this method returns the
     * empty map. Override this method to change their behavior.
     * </p>
     *
     * @return Map of types in this schema by name
     */
    protected Map<String, RelProtoDataType> getTypeMap() {
        return typeMap;
    }
}
