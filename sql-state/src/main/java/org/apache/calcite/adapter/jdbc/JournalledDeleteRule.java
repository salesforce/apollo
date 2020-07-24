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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilder;
import org.apache.calcite.adapter.jdbc.tools.JdbcRelBuilderFactory;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rex.RexNode;

public class JournalledDeleteRule extends AbstractForcedRule {

	public JournalledDeleteRule() {
		super(Operation.DELETE);
	}

	@Override
	public RelNode doApply(LogicalTableModify tableModify, JournalledJdbcTable journalTable,
			JdbcRelBuilderFactory relBuilderFactory) {

		String versionField = journalTable.getVersionField();
		String subsequentVersionField = journalTable.getSubsequentVersionField();

		JdbcRelBuilder relBuilder = relBuilderFactory.create(
				tableModify.getCluster(),
				tableModify.getTable().getRelOptSchema()
		);

		RelNode input = tableModify.getInput();

		// Bypass projection which removes version columns
		if (!(input instanceof LogicalProject)) {
			throw new IllegalStateException("Unknown Calcite DELETE structure");
		}
		relBuilder.push(input.getInput(0));

		JournalVersionType versionType = journalTable.getVersionType();
		if (!versionType.isValidSqlType(relBuilder.field(versionField).getType().getSqlTypeName())) {
			throw new IllegalStateException("Incorrect journalVersionType! Column 'version_number' is of type: "
					+ relBuilder.field(versionField).getType().getSqlTypeName()
					+ " but the journalVersionType is " + versionType);
		}

		RexNode newVersion = journalTable.getVersionType().incrementVersion(relBuilder, relBuilder.field(versionField));

		List<String> columnNames = new ArrayList<>();
		List<RexNode> sources = new ArrayList<>();
		sources.addAll(((LogicalProject) input).getProjects());
		columnNames.addAll(input.getRowType().getFieldNames());
		sources.add(newVersion);
		sources.add(newVersion);
		columnNames.add(versionField);
		columnNames.add(subsequentVersionField);
		relBuilder.project(sources, columnNames);

		relBuilder.insertCopying(
				tableModify,
				journalTable.getJournalTable()
		);

		return relBuilder.build();
	}
}
