// Generated by delombok at Thu Mar 11 18:53:10 PST 2021
package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.List;
import com.google.common.collect.Lists;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.definitions.PostgresTypes;
import io.quantumdb.core.schema.operations.CreateTable;
import io.quantumdb.core.schema.operations.SchemaOperations;

public class XmlCreateTable implements XmlOperation<CreateTable> {
    static final String TAG = "createTable";

    static XmlOperation<?> convert(XmlElement element) {
        checkArgument(element.getTag().equals(TAG));
        XmlCreateTable operation = new XmlCreateTable();
        operation.setTableName(element.getAttributes().get("tableName"));
        for (XmlElement child : element.getChildren()) {
            if (child.getTag().equals("columns")) {
                for (XmlElement subChild : child.getChildren()) {
                    operation.getColumns().add(XmlColumn.convert(subChild));
                }
            }
        }
        return operation;
    }

    private final List<XmlColumn> columns = Lists.newArrayList();
    private String                tableName;

    
    public XmlCreateTable() {
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof XmlCreateTable))
            return false;
        final XmlCreateTable other = (XmlCreateTable) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        final java.lang.Object this$columns = this.getColumns();
        final java.lang.Object other$columns = other.getColumns();
        if (this$columns == null ? other$columns != null : !this$columns.equals(other$columns))
            return false;
        return true;
    }

    
    public List<XmlColumn> getColumns() {
        return this.columns;
    }

    
    public String getTableName() {
        return this.tableName;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $tableName = this.getTableName();
        result = result * PRIME + ($tableName == null ? 43 : $tableName.hashCode());
        final java.lang.Object $columns = this.getColumns();
        result = result * PRIME + ($columns == null ? 43 : $columns.hashCode());
        return result;
    }

    
    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    @Override
    public CreateTable toOperation() {
        CreateTable operation = SchemaOperations.createTable(tableName);
        for (XmlColumn column : columns) {
            ColumnType type = PostgresTypes.from(column.getType());
            String defaultExpression = column.getDefaultExpression();
            List<Hint> hints = Lists.newArrayList();
            if (column.isPrimaryKey()) {
                hints.add(Hint.IDENTITY);
            }
            if (column.isAutoIncrement()) {
                hints.add(Hint.AUTO_INCREMENT);
            }
            if (!column.isNullable()) {
                hints.add(Hint.NOT_NULL);
            }
            Hint[] hintArray = hints.toArray(new Hint[hints.size()]);
            operation.with(column.getName(), type, defaultExpression, hintArray);
        }
        return operation;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "XmlCreateTable(tableName=" + this.getTableName() + ", columns=" + this.getColumns() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof XmlCreateTable;
    }
}
