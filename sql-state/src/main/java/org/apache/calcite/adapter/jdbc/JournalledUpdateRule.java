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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

public class JournalledUpdateRule extends AbstractForcedRule {

	public JournalledUpdateRule() {
		super(Operation.UPDATE);
	}

	@Override
	public RelNode doApply(LogicalTableModify tableModify, JournalledJdbcTable journalTable,
			JdbcRelBuilderFactory relBuilderFactory) {

		if (!(tableModify.getInput() instanceof LogicalProject)) {
			throw new IllegalStateException("Unknown Calcite UPDATE structure");
		}

		String versionField = journalTable.getVersionField();

		// Merge the Update's update column expression into the target INSERT
		LogicalProject project = (LogicalProject) tableModify.getInput();
		List<RexNode> desiredFields = new ArrayList<>();
		List<String> desiredNames = new ArrayList<>();

		for (Pair<RexNode, String> field : project.getNamedProjects()) {
			if (field.getKey() instanceof RexInputRef) {
				int index = tableModify.getUpdateColumnList().indexOf(field.getValue());
				if (index != -1) {
					desiredFields.add(tableModify.getSourceExpressionList().get(index));
				}
				else {
					desiredFields.add(field.getKey());
				}
				desiredNames.add(field.getValue());
			}
		}

		JdbcRelBuilder relBuilder = relBuilderFactory.create(
				tableModify.getCluster(),
				tableModify.getTable().getRelOptSchema()
		);

		relBuilder.push(project.getInput());

		JournalVersionType versionType = journalTable.getVersionType();
		if (!versionType.isValidSqlType(relBuilder.field(versionField).getType().getSqlTypeName())) {
			throw new IllegalStateException("Incorrect journalVersionType! Column 'version_number' is of type: "
					+ relBuilder.field(versionField).getType().getSqlTypeName()
					+ " but the journalVersionType is " + versionType);
		}
		if (versionType.updateRequiresExplicitVersion()) {
			RexNode newVersion = versionType.incrementVersion(relBuilder, relBuilder.field(versionField));
			desiredFields.add(newVersion);
			desiredNames.add(versionField);
		}

		relBuilder.project(desiredFields, desiredNames);

		// Convert the UPDATE into INSERT TableModify operations
		relBuilder.insertCopying(
				tableModify,
				journalTable.getJournalTable()
		);

		return relBuilder.build();
	}
}
