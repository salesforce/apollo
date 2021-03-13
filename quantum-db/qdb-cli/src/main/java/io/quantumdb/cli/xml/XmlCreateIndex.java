// Generated by delombok at Thu Mar 11 18:53:10 PST 2021
package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Optional;
import io.quantumdb.core.schema.operations.CreateIndex;
import io.quantumdb.core.schema.operations.SchemaOperations;

public class XmlCreateIndex implements XmlOperation<CreateIndex> {
	static final String TAG = "createIndex";

	static XmlOperation convert(XmlElement element) {
		checkArgument(element.getTag().equals(TAG));
		XmlCreateIndex operation = new XmlCreateIndex();
		operation.setTableName(element.getAttributes().get("tableName"));
		operation.setColumnNames(element.getAttributes().get("columnNames").split(","));
		Optional.ofNullable(element.getAttributes().get("unique")).map(Boolean.TRUE.toString()::equals).ifPresent(operation::setUnique);
		return operation;
	}

	private String tableName;
	private String[] columnNames;
	private boolean unique;

	@Override
	public CreateIndex toOperation() {
		return SchemaOperations.createIndex(tableName, unique, columnNames);
	}

	@java.lang.SuppressWarnings("all")
	public XmlCreateIndex() {
	}

	@java.lang.SuppressWarnings("all")
	public String getTableName() {
		return this.tableName;
	}

	@java.lang.SuppressWarnings("all")
	public String[] getColumnNames() {
		return this.columnNames;
	}

	@java.lang.SuppressWarnings("all")
	public boolean isUnique() {
		return this.unique;
	}

	@java.lang.SuppressWarnings("all")
	public void setTableName(final String tableName) {
		this.tableName = tableName;
	}

	@java.lang.SuppressWarnings("all")
	public void setColumnNames(final String[] columnNames) {
		this.columnNames = columnNames;
	}

	@java.lang.SuppressWarnings("all")
	public void setUnique(final boolean unique) {
		this.unique = unique;
	}

	@java.lang.Override
	@java.lang.SuppressWarnings("all")
	public boolean equals(final java.lang.Object o) {
		if (o == this) return true;
		if (!(o instanceof XmlCreateIndex)) return false;
		final XmlCreateIndex other = (XmlCreateIndex) o;
		if (!other.canEqual((java.lang.Object) this)) return false;
		if (this.isUnique() != other.isUnique()) return false;
		final java.lang.Object this$tableName = this.getTableName();
		final java.lang.Object other$tableName = other.getTableName();
		if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName)) return false;
		if (!java.util.Arrays.deepEquals(this.getColumnNames(), other.getColumnNames())) return false;
		return true;
	}

	@java.lang.SuppressWarnings("all")
	protected boolean canEqual(final java.lang.Object other) {
		return other instanceof XmlCreateIndex;
	}

	@java.lang.Override
	@java.lang.SuppressWarnings("all")
	public int hashCode() {
		final int PRIME = 59;
		int result = 1;
		result = result * PRIME + (this.isUnique() ? 79 : 97);
		final java.lang.Object $tableName = this.getTableName();
		result = result * PRIME + ($tableName == null ? 43 : $tableName.hashCode());
		result = result * PRIME + java.util.Arrays.deepHashCode(this.getColumnNames());
		return result;
	}

	@java.lang.Override
	@java.lang.SuppressWarnings("all")
	public java.lang.String toString() {
		return "XmlCreateIndex(tableName=" + this.getTableName() + ", columnNames=" + java.util.Arrays.deepToString(this.getColumnNames()) + ", unique=" + this.isUnique() + ")";
	}
}
