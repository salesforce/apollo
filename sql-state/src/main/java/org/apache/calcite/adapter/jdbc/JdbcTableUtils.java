/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.jdbc;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;

public class JdbcTableUtils {
	private JdbcTableUtils() {
		throw new UnsupportedOperationException();
	}

	private static Object get(Class<?> clazz, Object object, String fieldName) {
		try {
			Field field = clazz.getDeclaredField(fieldName);
			field.setAccessible(true);
			return field.get(object);
		}
		catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	static JdbcTable getJdbcTable(RelNode originalRel) {
		return (JdbcTable) get(RelOptTableImpl.class, originalRel.getTable(), "table");
	}

	static String getCatalogName(JdbcTable table) {
		return (String) get(JdbcTable.class, table, "jdbcCatalogName");
	}

	static String getSchemaName(JdbcTable table) {
		return (String) get(JdbcTable.class, table, "jdbcSchemaName");
	}

	static String getTableName(JdbcTable table) {
		// tableName is: [catalog,] [schema,] table
		SqlIdentifier identifier = table.tableName();
		return identifier.names.get(identifier.names.size() - 1);
	}

	public static List<String> getQualifiedName(RelOptTable sibling, Table table) {
		if (!(table instanceof JdbcTable)) {
			throw new UnsupportedOperationException();
		}

		List<String> name = new ArrayList<>();
		if (sibling != null) {
			name.addAll(sibling.getQualifiedName());
			name.remove(name.size() - 1);
		}
		name.add(getTableName((JdbcTable) table));
		return name;
	}

	static RelNode toRel(RelOptCluster cluster, RelOptSchema relOptSchema, JdbcTable table, List<String> qualifiedName) {
		RelOptTable.ToRelContext toRelContext = new RelOptTable.ToRelContext() {
			@Override
			public RelOptCluster getCluster() {
				return cluster;
			}

			@Override
			public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
				throw new UnsupportedOperationException();
			}

            @Override
            public List<RelHint> getTableHints() {
                return Collections.emptyList();
            }
		};

		return table.toRel(
				toRelContext,
				relOptSchema.getTableForMember(qualifiedName)
		);
	}

	public static RelNode toRel(RelOptCluster cluster, RelOptSchema relOptSchema, Table table, List<String> qualifiedName) {
		if (!(table instanceof JdbcTable)) {
			throw new UnsupportedOperationException();
		}
		return toRel(cluster, relOptSchema, (JdbcTable) table, qualifiedName);
	}
}
