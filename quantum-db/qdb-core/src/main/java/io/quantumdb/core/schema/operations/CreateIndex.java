
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.List;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * This SchemaOperation describes an operation which adds a non-existent column
 * to an already existing table.
 */
public class CreateIndex implements SchemaOperation {
    private final List<String> columns;
    private final String       tableName;
    private final boolean      unique;

    CreateIndex(String tableName, boolean unique, String... columns) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'");
        checkArgument(columns.length > 0, "You must specify at least one column");
        this.tableName = tableName;
        this.unique = unique;
        this.columns = Lists.newArrayList(columns);
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof CreateIndex))
            return false;
        final CreateIndex other = (CreateIndex) o;
        if (!other.canEqual(this))
            return false;
        if (this.isUnique() != other.isUnique())
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

    public ImmutableList<String> getColumns() {
        return ImmutableList.copyOf(columns);
    }

    
    public String getTableName() {
        return this.tableName;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        result = result * PRIME + (this.isUnique() ? 79 : 97);
        final java.lang.Object $tableName = this.getTableName();
        result = result * PRIME + ($tableName == null ? 43 : $tableName.hashCode());
        final java.lang.Object $columns = this.getColumns();
        result = result * PRIME + ($columns == null ? 43 : $columns.hashCode());
        return result;
    }

    
    public boolean isUnique() {
        return this.unique;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "CreateIndex(tableName=" + this.getTableName() + ", unique=" + this.isUnique() + ", columns="
                + this.getColumns() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof CreateIndex;
    }
}
