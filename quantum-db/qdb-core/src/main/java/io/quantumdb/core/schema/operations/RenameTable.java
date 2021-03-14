
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;

public class RenameTable implements SchemaOperation {
    private final String newTableName;
    private final String tableName;

    RenameTable(String tableName, String newTableName) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'tableName\'.");
        checkArgument(!Strings.isNullOrEmpty(newTableName), "You must specify a \'newTableName\'.");
        this.tableName = tableName;
        this.newTableName = newTableName;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof RenameTable))
            return false;
        final RenameTable other = (RenameTable) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        final java.lang.Object this$newTableName = this.getNewTableName();
        final java.lang.Object other$newTableName = other.getNewTableName();
        if (this$newTableName == null ? other$newTableName != null : !this$newTableName.equals(other$newTableName))
            return false;
        return true;
    }

    
    public String getNewTableName() {
        return this.newTableName;
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
        final java.lang.Object $newTableName = this.getNewTableName();
        result = result * PRIME + ($newTableName == null ? 43 : $newTableName.hashCode());
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "RenameTable(tableName=" + this.getTableName() + ", newTableName=" + this.getNewTableName() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof RenameTable;
    }
}
