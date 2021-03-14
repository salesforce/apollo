
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;

/**
 * This SchemaOperation describes an operation which adds a non-existent column
 * to an already existing table.
 */
public class DropIndex implements SchemaOperation {
    private final String[] columns;
    private final String   tableName;

    DropIndex(String tableName, String... columns) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'");
        checkArgument(columns.length > 0, "You must specify at least one \'column\'");
        this.tableName = tableName;
        this.columns = columns;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof DropIndex))
            return false;
        final DropIndex other = (DropIndex) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        if (!java.util.Arrays.deepEquals(this.getColumns(), other.getColumns()))
            return false;
        return true;
    }

    
    public String[] getColumns() {
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
        result = result * PRIME + java.util.Arrays.deepHashCode(this.getColumns());
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "DropIndex(tableName=" + this.getTableName() + ", columns="
                + java.util.Arrays.deepToString(this.getColumns()) + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof DropIndex;
    }
}
