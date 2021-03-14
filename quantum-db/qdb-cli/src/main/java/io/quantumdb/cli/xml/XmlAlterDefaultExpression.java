// Generated by delombok at Thu Mar 11 18:53:10 PST 2021
package io.quantumdb.cli.xml;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Map;
import io.quantumdb.core.schema.operations.AlterColumn;
import io.quantumdb.core.schema.operations.SchemaOperations;

public class XmlAlterDefaultExpression implements XmlOperation<AlterColumn> {
    static final String TAG = "alterDefaultExpression";

    static XmlOperation<?> convert(XmlElement element) {
        checkArgument(element.getTag().equals(TAG));
        Map<String, String> attributes = element.getAttributes();
        XmlAlterDefaultExpression operation = new XmlAlterDefaultExpression();
        operation.setTableName(attributes.get("tableName"));
        operation.setColumnName(attributes.get("columnName"));
        operation.setDefaultExpression(attributes.get("defaultExpression"));
        return operation;
    }

    private String columnName;
    private String defaultExpression;
    private String tableName;

    
    public XmlAlterDefaultExpression() {
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof XmlAlterDefaultExpression))
            return false;
        final XmlAlterDefaultExpression other = (XmlAlterDefaultExpression) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        final java.lang.Object this$columnName = this.getColumnName();
        final java.lang.Object other$columnName = other.getColumnName();
        if (this$columnName == null ? other$columnName != null : !this$columnName.equals(other$columnName))
            return false;
        final java.lang.Object this$defaultExpression = this.getDefaultExpression();
        final java.lang.Object other$defaultExpression = other.getDefaultExpression();
        if (this$defaultExpression == null ? other$defaultExpression != null
                : !this$defaultExpression.equals(other$defaultExpression))
            return false;
        return true;
    }

    
    public String getColumnName() {
        return this.columnName;
    }

    
    public String getDefaultExpression() {
        return this.defaultExpression;
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
        final java.lang.Object $columnName = this.getColumnName();
        result = result * PRIME + ($columnName == null ? 43 : $columnName.hashCode());
        final java.lang.Object $defaultExpression = this.getDefaultExpression();
        result = result * PRIME + ($defaultExpression == null ? 43 : $defaultExpression.hashCode());
        return result;
    }

    
    public void setColumnName(final String columnName) {
        this.columnName = columnName;
    }

    
    public void setDefaultExpression(final String defaultExpression) {
        this.defaultExpression = defaultExpression;
    }

    
    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    @Override
    public AlterColumn toOperation() {
        return SchemaOperations.alterColumn(tableName, columnName).modifyDefaultExpression(defaultExpression);
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "XmlAlterDefaultExpression(tableName=" + this.getTableName() + ", columnName=" + this.getColumnName()
                + ", defaultExpression=" + this.getDefaultExpression() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof XmlAlterDefaultExpression;
    }
}
