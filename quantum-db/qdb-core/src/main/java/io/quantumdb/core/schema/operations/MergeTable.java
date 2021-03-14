
package io.quantumdb.core.schema.operations;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Strings;

public class MergeTable implements SchemaOperation {
    private final String leftTableName;
    private final String rightTableName;
    private final String targetTableName;

    MergeTable(String leftTableName, String rightTableName, String targetTableName) {
        checkArgument(!Strings.isNullOrEmpty(leftTableName), "You must specify a \'leftTableName\'.");
        checkArgument(!Strings.isNullOrEmpty(rightTableName), "You must specify a \'rightTableName\'.");
        checkArgument(!Strings.isNullOrEmpty(targetTableName), "You must specify a \'targetTableName\'.");
        this.leftTableName = leftTableName;
        this.rightTableName = rightTableName;
        this.targetTableName = targetTableName;
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof MergeTable))
            return false;
        final MergeTable other = (MergeTable) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$leftTableName = this.getLeftTableName();
        final java.lang.Object other$leftTableName = other.getLeftTableName();
        if (this$leftTableName == null ? other$leftTableName != null : !this$leftTableName.equals(other$leftTableName))
            return false;
        final java.lang.Object this$rightTableName = this.getRightTableName();
        final java.lang.Object other$rightTableName = other.getRightTableName();
        if (this$rightTableName == null ? other$rightTableName != null
                : !this$rightTableName.equals(other$rightTableName))
            return false;
        final java.lang.Object this$targetTableName = this.getTargetTableName();
        final java.lang.Object other$targetTableName = other.getTargetTableName();
        if (this$targetTableName == null ? other$targetTableName != null
                : !this$targetTableName.equals(other$targetTableName))
            return false;
        return true;
    }

    
    public String getLeftTableName() {
        return this.leftTableName;
    }

    
    public String getRightTableName() {
        return this.rightTableName;
    }

    
    public String getTargetTableName() {
        return this.targetTableName;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $leftTableName = this.getLeftTableName();
        result = result * PRIME + ($leftTableName == null ? 43 : $leftTableName.hashCode());
        final java.lang.Object $rightTableName = this.getRightTableName();
        result = result * PRIME + ($rightTableName == null ? 43 : $rightTableName.hashCode());
        final java.lang.Object $targetTableName = this.getTargetTableName();
        result = result * PRIME + ($targetTableName == null ? 43 : $targetTableName.hashCode());
        return result;
    }

    @java.lang.Override
    
    public java.lang.String toString() {
        return "MergeTable(leftTableName=" + this.getLeftTableName() + ", rightTableName=" + this.getRightTableName()
                + ", targetTableName=" + this.getTargetTableName() + ")";
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof MergeTable;
    }
}
