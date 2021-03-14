
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.List;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.quantumdb.core.schema.definitions.Column.Hint;
import io.quantumdb.core.schema.definitions.ColumnType;

/**
 * This SchemaOperation describes an operation which creates a new table.
 */
public class CreateTable implements SchemaOperation {
    private final List<ColumnDefinition> columns;
    private final String                 tableName;

    CreateTable(String tableName) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'.");
        this.tableName = tableName;
        this.columns = Lists.newArrayList();
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof CreateTable))
            return false;
        final CreateTable other = (CreateTable) o;
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

    public ImmutableList<ColumnDefinition> getColumns() {
        return ImmutableList.copyOf(columns);
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

    @java.lang.Override
    
    public java.lang.String toString() {
        return "CreateTable(tableName=" + this.getTableName() + ", columns=" + this.getColumns() + ")";
    }

    public CreateTable with(String name, ColumnType type, Hint... hints) {
        return with(name, type, null, hints);
    }

    public CreateTable with(String name, ColumnType type, String defaultValueExpression, Hint... hints) {
        checkArgument(!Strings.isNullOrEmpty(name), "You must specify a \'name\'.");
        checkArgument(type != null, "You must specify a \'type\'.");
        columns.add(new ColumnDefinition(name, type, defaultValueExpression, hints));
        return this;
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof CreateTable;
    }
}
