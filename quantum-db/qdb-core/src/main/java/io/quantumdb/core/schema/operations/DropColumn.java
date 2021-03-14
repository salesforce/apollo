
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;

/**
 * This SchemaOperation describes an operation which drops an existing column
 * from an existing table.
 */
public class DropColumn implements SchemaOperation {
    private final String columnName;
    private final String tableName;

    DropColumn(String tableName, String columnName) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'.");
        checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a \'columnName\'.");
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof DropColumn))
            return false;
        final DropColumn other = (DropColumn) o;
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
        return true;
    }

    
    public String getColumnName() {
        return this.columnName;
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
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "DropColumn(tableName=" + this.getTableName() + ", columnName=" + this.getColumnName() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof DropColumn;
    }
}
