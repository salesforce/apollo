
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;

/**
 * This SchemaOperation describes an operation which copies an existing table to
 * another not yet existing table.
 */
public class CopyTable implements SchemaOperation {
    private final String sourceTableName;
    private final String targetTableName;

    CopyTable(String sourceTableName, String targetTableName) {
        checkArgument(!Strings.isNullOrEmpty(sourceTableName), "You must specify \'sourceTableName\'.");
        checkArgument(!Strings.isNullOrEmpty(targetTableName), "You must specify \'targetTableName\'.");
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof CopyTable))
            return false;
        final CopyTable other = (CopyTable) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$sourceTableName = this.getSourceTableName();
        final java.lang.Object other$sourceTableName = other.getSourceTableName();
        if (this$sourceTableName == null ? other$sourceTableName != null
                : !this$sourceTableName.equals(other$sourceTableName))
            return false;
        final java.lang.Object this$targetTableName = this.getTargetTableName();
        final java.lang.Object other$targetTableName = other.getTargetTableName();
        if (this$targetTableName == null ? other$targetTableName != null
                : !this$targetTableName.equals(other$targetTableName))
            return false;
        return true;
    }

    
    public String getSourceTableName() {
        return this.sourceTableName;
    }

    
    public String getTargetTableName() {
        return this.targetTableName;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $sourceTableName = this.getSourceTableName();
        result = result * PRIME + ($sourceTableName == null ? 43 : $sourceTableName.hashCode());
        final java.lang.Object $targetTableName = this.getTargetTableName();
        result = result * PRIME + ($targetTableName == null ? 43 : $targetTableName.hashCode());
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "CopyTable(sourceTableName=" + this.getSourceTableName() + ", targetTableName="
                + this.getTargetTableName() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof CopyTable;
    }
}
