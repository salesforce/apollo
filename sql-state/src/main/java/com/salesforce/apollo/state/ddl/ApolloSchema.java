/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.ddl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.materialize.Lattice;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

/**
 * @author hal.hildebrand
 *
 */
public class ApolloSchema implements SchemaPlus {

    public static ApolloSchema create(ChainSchema parentSchema, String name, DataSource ds, String catalog,
                                      String schema) {
        return new ApolloSchema(parentSchema, schema);
    }

    /**
     * Creates a JdbcSchema, taking credentials from a map.
     *
     * @param parentSchema Parent schema
     * @param name         Name
     * @param operand      Map of property/value pairs
     * @return A JdbcSchema
     */
    public static ApolloSchema create(ChainSchema parentSchema, String name, Map<String, Object> operand,
                                      DataSource ds) {
        String jdbcCatalog = (String) operand.get("jdbcCatalog");
        String jdbcSchema = (String) operand.get("jdbcSchema");
        return create(parentSchema, name, ds, jdbcCatalog, jdbcSchema);
    }

    /** Returns a suitable SQL dialect for the given data source. */
    public static SqlDialect createDialect(DataSource dataSource) {
        return Utils.DialectPool.INSTANCE.get(dataSource);
    }

    /** Creates a JDBC data source with the given specification. */
    public static DataSource dataSource(String url, String driverClassName, String username, String password) {
        if (url.startsWith("jdbc:hsqldb:")) {
            // Prevent hsqldb from screwing up java.util.logging.
            System.setProperty("hsqldb.reconfig_logging", "false");
        }
        return Utils.DataSourcePool.INSTANCE.get(url, driverClassName, username, password);
    }

    private static void close(Statement statement, ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private final Multimap<String, Function>    functions = MultimapBuilder.treeKeys().arrayListValues().build();
    private final ChainSchema                   parent;
    private final String                        schema;
    private Map<String, MaterializedTable>      tableMap  = new HashMap<>();
    private final Map<String, RelProtoDataType> typeMap   = new HashMap<>();

    public ApolloSchema(ChainSchema parentSchema, String schema) {
        super();
        this.parent = parentSchema;
        this.schema = schema;
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

    public boolean contentsHaveChangedSince(long lastCheck, long now) {
        return false;
    }

    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return Schemas.subSchemaExpression(parentSchema, name, JdbcSchema.class);
    }

    public final Set<String> getFunctionNames() {
        return getFunctions().keySet();
    }

    public final Collection<Function> getFunctions(String name) {
        return getFunctions().get(name); // never null
    }

    @Override
    public String getName() {
        return schema;
    }

    @Override
    public SchemaPlus getParentSchema() {
        return parent;
    }

    public ApolloSchema getSubSchema(String name) {
        return null;
    }

    public Set<String> getSubSchemaNames() {
        return ImmutableSet.of();
    }

    public Table getTable(String name) {
        return getTableMap(false).get(name);
    }

    public Set<String> getTableNames() {
        return getTableMap(true).keySet();
    }

    @Override
    public RelProtoDataType getType(String name) {
        return typeMap.get(name);
    }

    @Override
    public Set<String> getTypeNames() {
        return typeMap.keySet();
    }

    @Override
    public boolean isCacheEnabled() {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean isMutable() {
        return false;
    }

    @Override
    public void setCacheEnabled(boolean cache) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setPath(ImmutableList<ImmutableList<String>> path) {
        // TODO Auto-generated method stub

    }

    @Override
    public Schema snapshot(SchemaVersion version) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        // TODO Auto-generated method stub
        return null;
    }

    protected String getCatalog() {
        return "";
    }

    protected Multimap<String, Function> getFunctions() {
        return functions;
    }

    @SuppressWarnings("deprecation")
    RelProtoDataType getRelDataType(DatabaseMetaData metaData, String catalogName, String schemaName,
                                    String tableName) throws SQLException {
        final ResultSet resultSet = metaData.getColumns(catalogName, schemaName, tableName, null);

        // Temporary type factory, just for the duration of this method. Allowable
        // because we're creating a proto-type, not a type; before being used, the
        // proto-type will be copied into a real type factory.
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.FieldInfoBuilder fieldInfo = typeFactory.builder();
        while (resultSet.next()) {
            final String columnName = resultSet.getString(4);
            final int dataType = resultSet.getInt(5);
            final String typeString = resultSet.getString(6);
            final int precision;
            final int scale;
            switch (SqlType.valueOf(dataType)) {
            case TIMESTAMP:
            case TIME:
                precision = resultSet.getInt(9); // SCALE
                scale = 0;
                break;
            default:
                precision = resultSet.getInt(7); // SIZE
                scale = resultSet.getInt(9); // SCALE
                break;
            }
            RelDataType sqlType = sqlType(typeFactory, dataType, precision, scale, typeString);
            boolean nullable = resultSet.getInt(11) != DatabaseMetaData.columnNoNulls;
            fieldInfo.add(columnName, sqlType).nullable(nullable);
        }
        resultSet.close();
        return RelDataTypeImpl.proto(fieldInfo.build());
    }

    RelProtoDataType getRelDataType(String catalogName, String schemaName, String tableName) throws SQLException {
        try (Connection connection = parent.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            return getRelDataType(metaData, catalogName, schemaName, tableName);
        } finally {
            close(null, null);
        }
    }

    private ImmutableMap<String, MaterializedTable> computeTables() {
        ResultSet resultSet = null;
        try {
            DatabaseMetaData metaData = parent.getConnection().getMetaData();
            resultSet = metaData.getTables("", schema, null, null);
            final ImmutableMap.Builder<String, MaterializedTable> builder = ImmutableMap.builder();
            while (resultSet.next()) {
                final String tableName = resultSet.getString(3);
                final String tableTypeName = resultSet.getString(4);
                // Clean up table type. In particular, this ensures that 'SYSTEM TABLE',
                // returned by Phoenix among others, maps to TableType.SYSTEM_TABLE.
                // We know enum constants are upper-case without spaces, so we can't
                // make things worse.
                //
                // PostgreSQL returns tableTypeName==null for pg_toast* tables
                // This can happen if you start JdbcSchema off a "public" PG schema
                // The tables are not designed to be queried by users, however we do
                // not filter them as we keep all the other table types.
                final String tableTypeName2 = tableTypeName == null ? null
                        : tableTypeName.toUpperCase(Locale.ROOT).replace(' ', '_');
                final TableType tableType = Util.enumVal(TableType.OTHER, tableTypeName2);
                if (tableType == TableType.OTHER && tableTypeName2 != null) {
                    System.out.println("Unknown table type: " + tableTypeName2);
                }
                final MaterializedTable table = new MaterializedTable(this, tableName, tableType);
                builder.put(tableName, table);
            }
            return builder.build();
        } catch (SQLException e) {
            throw new RuntimeException("Exception while reading tables", e);
        } finally {
            close(null, resultSet);
        }
    }

    private synchronized ImmutableMap<String, MaterializedTable> getTableMap(boolean force) {
        if (force || tableMap == null) {
            tableMap = computeTables();
        }
        return (ImmutableMap<String, MaterializedTable>) tableMap;
    }

    /**
     * Given "INTEGER", returns BasicSqlType(INTEGER). Given "VARCHAR(10)", returns
     * BasicSqlType(VARCHAR, 10). Given "NUMERIC(10, 2)", returns
     * BasicSqlType(NUMERIC, 10, 2).
     */
    private RelDataType parseTypeString(RelDataTypeFactory typeFactory, String typeString) {
        int precision = -1;
        int scale = -1;
        int open = typeString.indexOf("(");
        if (open >= 0) {
            int close = typeString.indexOf(")", open);
            if (close >= 0) {
                String rest = typeString.substring(open + 1, close);
                typeString = typeString.substring(0, open);
                int comma = rest.indexOf(",");
                if (comma >= 0) {
                    precision = Integer.parseInt(rest.substring(0, comma));
                    scale = Integer.parseInt(rest.substring(comma));
                } else {
                    precision = Integer.parseInt(rest);
                }
            }
        }
        try {
            final SqlTypeName typeName = SqlTypeName.valueOf(typeString);
            return typeName.allowsPrecScale(true, true) ? typeFactory.createSqlType(typeName, precision, scale)
                           : typeName.allowsPrecScale(true, false) ? typeFactory.createSqlType(typeName, precision)
                           : typeFactory.createSqlType(typeName);
        } catch (IllegalArgumentException e) {
            return typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);
        }
    }

    private RelDataType sqlType(RelDataTypeFactory typeFactory, int dataType, int precision, int scale,
                                String typeString) {
        // Fall back to ANY if type is unknown
        final SqlTypeName sqlTypeName = Util.first(SqlTypeName.getNameForJdbcType(dataType), SqlTypeName.ANY);
        switch (sqlTypeName) {
        case ARRAY:
            RelDataType component = null;
            if (typeString != null && typeString.endsWith(" ARRAY")) {
                // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type
                // "INTEGER".
                final String remaining = typeString.substring(0, typeString.length() - " ARRAY".length());
                component = parseTypeString(typeFactory, remaining);
            }
            if (component == null) {
                component = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true);
            }
            return typeFactory.createArrayType(component, -1);
        default:
            break;
        }
        if (precision >= 0 && scale >= 0 && sqlTypeName.allowsPrecScale(true, true)) {
            return typeFactory.createSqlType(sqlTypeName, precision, scale);
        } else if (precision >= 0 && sqlTypeName.allowsPrecNoScale()) {
            return typeFactory.createSqlType(sqlTypeName, precision);
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            return typeFactory.createSqlType(sqlTypeName);
        }
    }

    public JdbcConvention getConvention() {
        // TODO Auto-generated method stub
        return null;
    }

    public DataSource getDatasource() {
        return parent.getDatasource();
    }
}
