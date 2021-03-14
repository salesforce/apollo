
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType;

/**
 * This SchemaOperation describes an operation which adds a non-existent column
 * to an already existing table.
 */
public class AddColumn implements SchemaOperation {
    private final ColumnDefinition columnDefinition;
    private final String           tableName;

    AddColumn(String tableName, String columnName, ColumnType type, String defaultExpression, Hint... hints) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'");
        checkArgument(!Strings.isNullOrEmpty(columnName), "You must specify a \'columnName\'");
        checkArgument(type != null, "You must specify a \'type\'");
        this.tableName = tableName;
        this.columnDefinition = new ColumnDefinition(columnName, type, defaultExpression, hints);
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof AddColumn))
            return false;
        final AddColumn other = (AddColumn) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        final java.lang.Object this$columnDefinition = this.getColumnDefinition();
        final java.lang.Object other$columnDefinition = other.getColumnDefinition();
        if (this$columnDefinition == null ? other$columnDefinition != null
                : !this$columnDefinition.equals(other$columnDefinition))
            return false;
        return true;
    }

    
    public ColumnDefinition getColumnDefinition() {
        return this.columnDefinition;
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
        final java.lang.Object $columnDefinition = this.getColumnDefinition();
        result = result * PRIME + ($columnDefinition == null ? 43 : $columnDefinition.hashCode());
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "AddColumn(tableName=" + this.getTableName() + ", columnDefinition=" + this.getColumnDefinition() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof AddColumn;
    }
}
