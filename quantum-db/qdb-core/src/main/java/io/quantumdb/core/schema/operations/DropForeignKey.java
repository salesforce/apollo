
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;

public class DropForeignKey implements SchemaOperation {
    private final String foreignKeyName;
    private final String tableName;

    DropForeignKey(String tableName, String foreignKeyName) {
        checkArgument(!Strings.isNullOrEmpty(tableName), "You must specify a \'table\'.");
        checkArgument(!Strings.isNullOrEmpty(foreignKeyName), "You must specify a \'foreignKeyName\'.");
        this.tableName = tableName;
        this.foreignKeyName = foreignKeyName;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof DropForeignKey))
            return false;
        final DropForeignKey other = (DropForeignKey) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$tableName = this.getTableName();
        final java.lang.Object other$tableName = other.getTableName();
        if (this$tableName == null ? other$tableName != null : !this$tableName.equals(other$tableName))
            return false;
        final java.lang.Object this$foreignKeyName = this.getForeignKeyName();
        final java.lang.Object other$foreignKeyName = other.getForeignKeyName();
        if (this$foreignKeyName == null ? other$foreignKeyName != null
                : !this$foreignKeyName.equals(other$foreignKeyName))
            return false;
        return true;
    }

    
    public String getForeignKeyName() {
        return this.foreignKeyName;
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
        final java.lang.Object $foreignKeyName = this.getForeignKeyName();
        result = result * PRIME + ($foreignKeyName == null ? 43 : $foreignKeyName.hashCode());
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "DropForeignKey(tableName=" + this.getTableName() + ", foreignKeyName=" + this.getForeignKeyName() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof DropForeignKey;
    }
}
