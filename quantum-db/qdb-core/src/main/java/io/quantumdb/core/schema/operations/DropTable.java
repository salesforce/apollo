
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;

/**
 * This SchemaOperation describes an operation which drops an existing table
 * from a catalog.
 */
public class DropTable implements SchemaOperation {
    private final String tableName;

    DropTable(String tableName) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'.");
        this.tableName = tableName;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof DropTable))
            return false;
        final DropTable other = (DropTable) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        return true;
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
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "DropTable(tableName=" + this.getTableName() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof DropTable;
    }
}
